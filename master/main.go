package main

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"os"
	"time"
)

// plain token the GUI sends — server only stores its SHA256 hash
const authToken = "my-secret-token-123"

// authTokenHash is the SHA256 hash of authToken, computed once at startup
var authTokenHash = fmt.Sprintf("%x", sha256.Sum256([]byte(authToken)))

// add or remove slaves here freely
var allSlaves = []string{
	"http://localhost:8081", // slave-go
	"http://localhost:8082", // slave-python
}

// corsMiddleware adds CORS headers so the browser GUI can talk to the master
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

func main() {

	port := os.Getenv("PORT")
	if port == "" {
		port = "8095"
	}

	// --- Load metadata from disk ---
	meta := loadMetadata()

	// --- Slave state tracker ---
	state := newSlaveState(allSlaves)

	// --- Initial ping to see who's online ---
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

	// --- Start heartbeat every 5 seconds ---
	startHeartbeat(allSlaves, state, meta, 5*time.Second)

	// --- Serve GUI ---
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "../gui/index.html")
	})

	// --- Routes (all wrapped with CORS) ---
	http.HandleFunc("/db/create", corsMiddleware(handleCreateDB(allSlaves, state)))
	http.HandleFunc("/db/drop", corsMiddleware(handleDropDB(allSlaves, state)))
	http.HandleFunc("/tables/create", corsMiddleware(handleCreateTable(meta, allSlaves, state)))
	http.HandleFunc("/tables/drop", corsMiddleware(handleDropTable(meta, allSlaves, state)))
	http.HandleFunc("/tables/insert", corsMiddleware(handleInsert(allSlaves, state)))
	http.HandleFunc("/tables/select", corsMiddleware(handleSelect(meta, allSlaves, state)))
	http.HandleFunc("/tables/update", corsMiddleware(handleUpdate(allSlaves, state)))
	http.HandleFunc("/tables/delete", corsMiddleware(handleDelete(allSlaves, state)))
	http.HandleFunc("/health", corsMiddleware(handleHealth(state)))

	fmt.Println("Master running on port " + port + "...")

	if err := http.ListenAndServe("0.0.0.0:"+port, nil); err != nil {
		fmt.Println("✗ Server error:", err)
	}
}
