package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
)

const authToken = "my-secret-token-123"

var authTokenHash = fmt.Sprintf("%x", sha256.Sum256([]byte(authToken)))

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

	// auto_increment settings — controlled by env so each slave gets a unique
	// offset without hardcoding odd/even. With 2 slaves:
	//   slave-go:  SLAVE_INDEX=0 → offset=1, increment=2 → IDs 1,3,5,…
	//   slave-py:  SLAVE_INDEX=1 → offset=2, increment=2 → IDs 2,4,6,…
	// Adding a third slave: SLAVE_INDEX=2 → offset=3, increment=3 on all.
	slaveIndex := os.Getenv("SLAVE_INDEX")
	if slaveIndex == "" {
		slaveIndex = "0"
	}
	totalSlaves := os.Getenv("TOTAL_SLAVES")
	if totalSlaves == "" {
		totalSlaves = "2"
	}
	db.Exec("SET GLOBAL auto_increment_increment = " + totalSlaves)
	db.Exec("SET GLOBAL auto_increment_offset = " + func() string {
		var idx int
		fmt.Sscan(slaveIndex, &idx)
		return fmt.Sprintf("%d", idx+1)
	}())

	localMeta := &Metadata{Shards: make(map[string]map[string]ShardInfo)}

	http.HandleFunc("/ping", pingHandler)
	http.HandleFunc("/internal/exec", execHandler(db))
	http.HandleFunc("/internal/metadata", metadataHandler(localMeta))
	http.HandleFunc("/internal/sync-metadata", syncMetadataHandler(localMeta))

	// /promote reports whether this slave is acting as master.
	// The GUI polls this on all known nodes to find the active master.
	http.HandleFunc("/promote", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(map[string]bool{
			"acting_master": atomic.LoadInt32(&isMaster) == 1,
		})
	})

	fmt.Println("Slave running on port " + port + "...")
	go watchMaster(masterURL)

	if err := http.ListenAndServe("0.0.0.0:"+port, nil); err != nil {
		fmt.Println("✗ Server error:", err)
	}
}
