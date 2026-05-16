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

// peerSlaveURLs holds the URLs of all OTHER slaves (not this one).
// Used by watchMaster to check for split-brain before self-promoting.
// FIX (#3): populated from PEER_SLAVE_URLS env var at startup.
var peerSlaveURLs []string

// readCloser wraps a byte slice so we can use it as an io.ReadCloser in requests.
func readCloser(b []byte) io.ReadCloser {
	return io.NopCloser(bytes.NewReader(b))
}

// mergeRows deduplicates rows from multiple sources by id.
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
	/*
			       const KNOWN_NODES = [
		      "http://172.20.10.2:8085", // master
		      "http://172.20.10.7:8081", // slave go
		      "http://172.20.10.1:8082" //slave python
		    ]
	*/
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		dsn = "root:rootroot@tcp(127.0.0.1:3306)/"
	}

	masterURL := os.Getenv("MASTER_URL")
	if masterURL == "" {
		masterURL = "http://172.20.10.2:8085"
	}

	// check whether another slave has already promoted before self-promoting.
	peerSlaveURLs = []string{"http://172.20.10.4:8082"}

	db, err := openDB(dsn)
	if err != nil {
		fmt.Println("✗ Could not connect to MySQL:", err)
		os.Exit(1)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	fmt.Println("✓ Connected to MySQL")

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
	offset := idx + 1

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
	// FIX (#10): also set SESSION-level variables so the current connection
	// uses the correct offset immediately (GLOBAL only affects new connections).
	if _, err := db.Exec("SET SESSION auto_increment_increment = " + totalSlaves); err != nil {
		fmt.Printf("  ⚠ Could not set SESSION auto_increment_increment: %v\n", err)
	}
	if _, err := db.Exec(fmt.Sprintf("SET SESSION auto_increment_offset = %d", offset)); err != nil {
		fmt.Printf("  ⚠ Could not set SESSION auto_increment_offset: %v\n", err)
	}
	fmt.Printf("✓ auto_increment_increment=%s, auto_increment_offset=%d\n", totalSlaves, offset)

	localMeta := &Metadata{Shards: make(map[string]map[string]ShardInfo)}

	http.HandleFunc("/ping", pingHandler)
	http.HandleFunc("/internal/exec", execHandler(db))
	http.HandleFunc("/internal/metadata", metadataHandler(localMeta))
	http.HandleFunc("/internal/sync-metadata", syncMetadataHandler(localMeta))

	http.HandleFunc("/promote", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(map[string]bool{
			"acting_master": atomic.LoadInt32(&isMaster) == 1,
		})
	})

	registerMasterRoutes(db, localMeta)

	fmt.Println("Slave running on port " + port + "...")

	go watchMaster(masterURL, db, localMeta)

	if err := http.ListenAndServe("172.20.10.7:"+port, nil); err != nil {
		fmt.Println("✗ Server error:", err)
	}
}
