package main

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"os"
	"time"
)

const authToken = "my-secret-token-123"

var authTokenHash = fmt.Sprintf("%x", sha256.Sum256([]byte(authToken)))

var allSlaves = []string{
	"http://localhost:8081", // slave-go
	"http://localhost:8082", // slave-python
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

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8095"
	}

	meta := loadMetadata()
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

	// /ping lets the GUI discover which node is alive (same endpoint slaves expose)
	http.HandleFunc("/ping", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if !authenticate(r) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("pong"))
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
	if err := http.ListenAndServe("0.0.0.0:"+port, nil); err != nil {
		fmt.Println("✗ Server error:", err)
	}
}
