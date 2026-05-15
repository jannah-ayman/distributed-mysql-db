package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync/atomic"
)

const authToken = "my-secret-token-123"

var authTokenHash = fmt.Sprintf("%x", sha256.Sum256([]byte(authToken)))

// readCloser wraps a byte slice so we can use it as an io.ReadCloser in requests.
func readCloser(b []byte) io.ReadCloser {
	return io.NopCloser(bytes.NewReader(b))
}

// mergeRows deduplicates rows from multiple sources by id.
// Copied here so the promoted slave's select handler can use it.
func mergeRows(allRows ...[]map[string]any) []map[string]any {
	seen := make(map[string]bool)
	var merged []map[string]any
	for _, rows := range allRows {
		for _, row := range rows {
			id := fmt.Sprintf("%v", row["id"])
			if !seen[id] {
				seen[id] = true
				merged = append(merged, row)
			}
		}
	}
	return merged
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}

	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		dsn = "root:password@tcp(127.0.0.1:3306)/"
	}

	masterURL := os.Getenv("MASTER_URL")
	if masterURL == "" {
		masterURL = "http://localhost:8095"
	}

	db, err := openDB(dsn)
	if err != nil {
		fmt.Println("✗ Could not connect to MySQL:", err)
		os.Exit(1)
	}
	defer db.Close()
	fmt.Println("✓ Connected to MySQL")

	// FIX (#12): auto_increment settings must succeed or IDs will collide across
	// slaves. Previously db.Exec errors were discarded, so a MySQL user without
	// SUPER/SYSTEM_VARIABLES_ADMIN privilege would silently produce overlapping
	// IDs, causing primary-key conflicts on INSERT.
	// We now check both SET GLOBAL calls and crash loudly if they fail.
	// If your MySQL user lacks the privilege, grant it with:
	//   GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'youruser'@'%';
	slaveIndex := os.Getenv("SLAVE_INDEX")
	if slaveIndex == "" {
		slaveIndex = "0"
	}
	totalSlaves := os.Getenv("TOTAL_SLAVES")
	if totalSlaves == "" {
		totalSlaves = "2"
	}

	var idx int
	fmt.Sscan(slaveIndex, &idx)
	offset := idx + 1 // slave[0] → offset 1, slave[1] → offset 2, etc.

	if _, err := db.Exec("SET GLOBAL auto_increment_increment = " + totalSlaves); err != nil {
		fmt.Printf("✗ Could not set auto_increment_increment: %v\n", err)
		fmt.Println("  Hint: GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO your MySQL user")
		os.Exit(1)
	}
	if _, err := db.Exec(fmt.Sprintf("SET GLOBAL auto_increment_offset = %d", offset)); err != nil {
		fmt.Printf("✗ Could not set auto_increment_offset: %v\n", err)
		fmt.Println("  Hint: GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO your MySQL user")
		os.Exit(1)
	}
	fmt.Printf("✓ auto_increment_increment=%s, auto_increment_offset=%d\n", totalSlaves, offset)

	localMeta := &Metadata{Shards: make(map[string]map[string]ShardInfo)}

	// Register slave-only routes.
	http.HandleFunc("/ping", pingHandler)
	http.HandleFunc("/internal/exec", execHandler(db))
	http.HandleFunc("/internal/metadata", metadataHandler(localMeta))
	http.HandleFunc("/internal/sync-metadata", syncMetadataHandler(localMeta))

	// /promote reports whether this slave is acting as master.
	http.HandleFunc("/promote", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(map[string]bool{
			"acting_master": atomic.LoadInt32(&isMaster) == 1,
		})
	})

	// Pre-register master-style routes (gated by masterGuard — inactive until promoted).
	registerMasterRoutes(db, localMeta)

	fmt.Println("Slave running on port " + port + "...")

	// Pass db and localMeta so watchMaster can push metadata back on master recovery.
	go watchMaster(masterURL, db, localMeta)

	if err := http.ListenAndServe("0.0.0.0:"+port, nil); err != nil {
		fmt.Println("✗ Server error:", err)
	}
}
