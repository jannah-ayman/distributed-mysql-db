package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

const authToken = "my-secret-token-123"

var authTokenHash = fmt.Sprintf("%x", sha256.Sum256([]byte(authToken)))

var allSlaves = []string{
	// "http://localhost:8081",     // slave-go
	"http://192.168.1.108:8082", // slave-python
}

func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Auth-Token")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next(w, r)
	}
}

// fetchMetadataFromSlaves tries to pull metadata from any online slave.
// Used on master restart so it picks up any schema changes that happened
// while it was down (e.g. a slave was promoted and created a table).
func fetchMetadataFromSlaves(slaves []string) *Metadata {
	client := http.Client{Timeout: 5 * time.Second}
	for _, url := range slaves {
		req, err := http.NewRequest("GET", url+"/internal/metadata", nil)
		if err != nil {
			continue
		}
		req.Header.Set("X-Auth-Token", authToken)
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		var m Metadata
		if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
			resp.Body.Close()
			continue
		}
		resp.Body.Close()
		if len(m.Shards) > 0 {
			fmt.Printf("  ✓ Pulled metadata from %s (%d tables)\n", url, len(m.Shards))
			return &m
		}
	}
	return nil
}

// readCloser is a small helper used in pushes (avoids import of bytes in other files).
func readCloser(b []byte) *bytes.Reader {
	return bytes.NewReader(b)
}
func slaveWasPromoted(slaves []string) bool {
	client := http.Client{Timeout: 3 * time.Second}
	for _, url := range slaves {
		req, err := http.NewRequest("GET", url+"/promote", nil)
		if err != nil {
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		var result map[string]bool
		json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		if result["acting_master"] {
			fmt.Printf("  ✓ %s was acting as master\n", url)
			return true
		}
	}
	return false
}
func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8095"
	}

	// Load metadata from disk first.
	meta := loadMetadata()

	// If disk metadata is empty, try to pull from a slave — the master may have
	// crashed while a promoted slave handled requests and updated the schema.
	fmt.Println("Checking if a slave was promoted while master was down...")
	if slaveWasPromoted(allSlaves) {
		fmt.Println("  A slave was acting as master — pulling its metadata...")
		if pulled := fetchMetadataFromSlaves(allSlaves); pulled != nil {
			for table, shards := range pulled.Shards {
				if _, exists := meta.Shards[table]; !exists {
					meta.Shards[table] = shards
					fmt.Printf("  ✓ Learned about table %s from promoted slave\n", table)
				}
			}
			saveMetadata(meta)
		}
	}

	state := newSlaveState(allSlaves)

	fmt.Println("Checking slaves...")
	ch := make(chan SlaveStatus, len(allSlaves))
	for _, url := range allSlaves {
		go checkSlave(url, ch)
	}
	for range allSlaves {
		s := <-ch
		state.setOnline(s.URL, s.Online)
		if s.Online {
			fmt.Printf("  ✓ %s is online\n", s.URL)
		} else {
			fmt.Printf("  ✗ %s is offline\n", s.URL)
		}
	}

	startHeartbeat(allSlaves, state, meta, 5*time.Second)

	http.HandleFunc("/ping", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if !authenticate(r) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("pong"))
	}))

	// Also accept metadata pushes from a recovering slave.
	http.HandleFunc("/internal/sync-metadata", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if !authenticate(r) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var incoming Metadata
		if err := json.NewDecoder(r.Body).Decode(&incoming); err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}
		// Merge: keep any shards the master already knows about, add new ones.
		for table, shards := range incoming.Shards {
			if _, exists := meta.Shards[table]; !exists {
				meta.Shards[table] = shards
				fmt.Printf("  ✓ Master learned about table %s from slave push\n", table)
			}
		}
		saveMetadata(meta)
		w.WriteHeader(http.StatusOK)
		fmt.Println("  ✓ Metadata received from recovering slave")
	}))

	http.HandleFunc("/db/create", corsMiddleware(handleCreateDB(allSlaves, state)))
	http.HandleFunc("/db/drop", corsMiddleware(handleDropDB(allSlaves, state)))
	http.HandleFunc("/tables/create", corsMiddleware(handleCreateTable(meta, allSlaves, state)))
	http.HandleFunc("/tables/drop", corsMiddleware(handleDropTable(meta, allSlaves, state)))
	http.HandleFunc("/tables/insert", corsMiddleware(handleInsert(allSlaves, state)))
	http.HandleFunc("/tables/select", corsMiddleware(handleSelect(allSlaves, state)))
	http.HandleFunc("/tables/update", corsMiddleware(handleUpdate(allSlaves, state)))
	http.HandleFunc("/tables/delete", corsMiddleware(handleDelete(allSlaves, state)))
	http.HandleFunc("/health", corsMiddleware(handleHealth(state)))

	fmt.Println("Master running on port " + port + "...")
	if err := http.ListenAndServe("192.168.1.105:"+port, nil); err != nil {
		fmt.Println("✗ Server error:", err)
	}
}
