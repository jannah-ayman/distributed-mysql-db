package main

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"os"
)

const authToken = "my-secret-token-123"

var authTokenHash = fmt.Sprintf("%x", sha256.Sum256([]byte(authToken)))

func main() {

	// --- Config from env or defaults ---
	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}

	// MySQL DSN: user:password@tcp(host:port)/
	// no database selected here — we use db_name per request
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		dsn = "root:password@tcp(127.0.0.1:3306)/"
	}

	// --- Connect to MySQL ---
	db, err := openDB(dsn)
	if err != nil {
		fmt.Println("✗ Could not connect to MySQL:", err)
		os.Exit(1)
	}
	defer db.Close()
	fmt.Println("✓ Connected to MySQL")
	db.Exec("SET GLOBAL auto_increment_increment = 2")
	db.Exec("SET GLOBAL auto_increment_offset = 1")

	// --- Local metadata copy (master will sync this) ---
	localMeta := &Metadata{
		Shards: make(map[string]map[string]ShardInfo),
	}

	// --- Routes ---
	http.HandleFunc("/ping", pingHandler)
	http.HandleFunc("/internal/exec", execHandler(db))
	http.HandleFunc("/internal/metadata", metadataHandler(localMeta))
	http.HandleFunc("/internal/sync-metadata", syncMetadataHandler(localMeta))

	fmt.Println("Slave (Go) running on port " + port + "...")

	if err := http.ListenAndServe("0.0.0.0:"+port, nil); err != nil {
		fmt.Println("✗ Server error:", err)
	}
}
