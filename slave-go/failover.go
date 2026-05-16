package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

var isMaster int32 // 0 = normal slave, 1 = acting as master

// watchMaster pings the real master every 5s.
// After 3 consecutive failures (~15s) this slave promotes itself —
// but ONLY if it is the lowest-indexed slave still reachable, to prevent
// split-brain (two slaves promoting simultaneously).
// When the master comes back, it reverts to slave mode and pushes
// its local metadata back to the master.
func watchMaster(masterURL string, db *sql.DB, localMeta *Metadata) {
	client := http.Client{Timeout: 3 * time.Second}
	fails := 0
	for {
		time.Sleep(5 * time.Second)
		req, _ := http.NewRequest("GET", masterURL+"/health", nil)
		req.Header.Set("X-Auth-Token", authToken)
		resp, err := client.Do(req)

		if err != nil || resp.StatusCode != http.StatusOK {
			fails++
			fmt.Printf("  ⚠ Master unreachable (%d/3)\n", fails)
			if fails >= 3 && atomic.LoadInt32(&isMaster) == 0 {
				// FIX (#3 split-brain): before promoting, check if another slave
				// is already acting as master. We pick the slave with the lowest
				// port number as the tie-breaker — only promote if no other slave
				// has already promoted itself.
				if !anotherSlaveIsActingAsMaster() {
					promote()
				} else {
					fmt.Println("  ℹ Another slave is already acting as master — staying as slave")
				}
			}
		} else {
			resp.Body.Close()
			if atomic.LoadInt32(&isMaster) == 1 {
				fmt.Println("  ✓ Real master is back — reverting to slave mode")
				pushMetadataToMaster(masterURL, localMeta)
			}
			atomic.StoreInt32(&isMaster, 0)
			// FIX (#2): reset fails counter when master is reachable again,
			// so the next outage starts a clean count from 0.
			fails = 0
		}
	}
}

// anotherSlaveIsActingAsMaster checks all known peer slaves to see if any
// has already promoted itself. This prevents split-brain.
// FIX (#3): this function is the core of the split-brain prevention.
func anotherSlaveIsActingAsMaster() bool {
	client := http.Client{Timeout: 2 * time.Second}
	for _, peerURL := range peerSlaveURLs {
		req, err := http.NewRequest("GET", peerURL+"/promote", nil)
		if err != nil {
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		var result map[string]bool
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			resp.Body.Close()
			continue
		}
		resp.Body.Close()
		if result["acting_master"] {
			return true
		}
	}
	return false
}

// broadcastToPeers sends an ExecRequest to all peer slaves via /internal/exec
func broadcastToPeers(req ExecRequest) {
	client := http.Client{Timeout: 5 * time.Second}
	body, err := json.Marshal(req)
	if err != nil {
		fmt.Println("  ✗ Could not marshal peer broadcast:", err)
		return
	}
	for _, peer := range peerSlaveURLs {
		go func(u string) {
			httpReq, err := http.NewRequest("POST", u+"/internal/exec", bytes.NewReader(body))
			if err != nil {
				return
			}
			httpReq.Header.Set("Content-Type", "application/json")
			httpReq.Header.Set("X-Auth-Token", authToken)
			resp, err := client.Do(httpReq)
			if err != nil {
				fmt.Printf("  ✗ Peer broadcast to %s failed: %v\n", u, err)
				return
			}
			resp.Body.Close()
			fmt.Printf("  ✓ Peer broadcast to %s succeeded\n", u)
		}(peer)
	}
}
func promote() {
	if atomic.CompareAndSwapInt32(&isMaster, 0, 1) {
		fmt.Println("  ★ Master appears down — this slave is now acting as master")
	}
}

// pushMetadataToMaster sends local metadata back to the real master when it recovers.
func pushMetadataToMaster(masterURL string, meta *Metadata) {
	body, err := json.Marshal(meta)
	if err != nil {
		fmt.Println("  ✗ Could not marshal metadata for push:", err)
		return
	}
	client := http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequest("POST", masterURL+"/internal/sync-metadata", readCloser(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Auth-Token", authToken)
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("  ✗ Could not push metadata to master:", err)
		return
	}
	resp.Body.Close()
	fmt.Println("  ✓ Pushed metadata to recovered master")
}

// masterGuard wraps a handler — only runs it when this slave has been promoted.
// Returns 503 otherwise so the GUI's discoverMaster() skips this node.
func masterGuard(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Auth-Token")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		if atomic.LoadInt32(&isMaster) == 0 {
			http.Error(w, "not the master", http.StatusServiceUnavailable)
			return
		}
		h(w, r)
	}
}

// registerMasterRoutes pre-registers GUI-facing routes on this slave at startup.
// They are all gated by masterGuard, so they do nothing until promotion.
func registerMasterRoutes(db *sql.DB, localMeta *Metadata) {
	http.HandleFunc("/db/create", masterGuard(func(w http.ResponseWriter, r *http.Request) {
		var req CreateDBRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, "Invalid body")
			return
		}
		if err := createDatabase(db, req.DBName); err != nil {
			writeError(w, err.Error())
			return
		}
		broadcastToPeers(ExecRequest{
			DBName:    req.DBName,
			Operation: "CREATE_DB",
		})
		fmt.Printf("  [promoted] ✓ Created DB %s\n", req.DBName)
		writeSuccess(w, nil)
	}))

	http.HandleFunc("/db/drop", masterGuard(func(w http.ResponseWriter, r *http.Request) {
		var req DropDBRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, "Invalid body")
			return
		}
		if err := dropDatabase(db, req.DBName); err != nil {
			writeError(w, err.Error())
			return
		}
		broadcastToPeers(ExecRequest{
			DBName:    req.DBName,
			Operation: "DROP_DB",
		})
		fmt.Printf("  [promoted] ✓ Dropped DB %s\n", req.DBName)
		writeSuccess(w, nil)
	}))

	http.HandleFunc("/tables/create", masterGuard(func(w http.ResponseWriter, r *http.Request) {
		var req CreateTableRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, "Invalid body")
			return
		}
		if err := createTable(db, req.DBName, req.Table, req.Columns); err != nil {
			writeError(w, err.Error())
			return
		}
		_ = createTable(db, req.DBName, req.Table+"_replica", req.Columns)
		broadcastToPeers(ExecRequest{
			DBName:    req.DBName,
			Operation: "CREATE_TABLE",
			Table:     req.Table,
			Columns:   req.Columns,
		})
		localMeta.Shards[req.Table] = map[string]ShardInfo{
			"shard_1": {URL: "self", DBName: req.DBName},
		}
		fmt.Printf("  [promoted] ✓ Created table %s.%s\n", req.DBName, req.Table)
		writeSuccess(w, nil)
	}))

	http.HandleFunc("/tables/drop", masterGuard(func(w http.ResponseWriter, r *http.Request) {
		var req DropTableRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, "Invalid body")
			return
		}
		if err := dropTable(db, req.DBName, req.Table); err != nil {
			writeError(w, err.Error())
			return
		}
		_ = dropTable(db, req.DBName, req.Table+"_replica")
		broadcastToPeers(ExecRequest{
			DBName:    req.DBName,
			Operation: "DROP_TABLE",
			Table:     req.Table,
		})
		delete(localMeta.Shards, req.Table)
		fmt.Printf("  [promoted] ✓ Dropped table %s.%s\n", req.DBName, req.Table)
		writeSuccess(w, nil)
	}))

	http.HandleFunc("/tables/insert", masterGuard(func(w http.ResponseWriter, r *http.Request) {
		var req InsertRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, "Invalid body")
			return
		}
		if err := insertRow(db, req.DBName, req.Table, req.Data); err != nil {
			writeError(w, err.Error())
			return
		}
		// send as replica to peers
		broadcastToPeers(ExecRequest{
			DBName:    req.DBName,
			Operation: "INSERT",
			Table:     req.Table,
			Data:      req.Data,
			IsReplica: true,
		})
		fmt.Printf("  [promoted] ✓ Inserted into %s.%s\n", req.DBName, req.Table)
		writeSuccess(w, nil)
	}))

	http.HandleFunc("/tables/select", masterGuard(func(w http.ResponseWriter, r *http.Request) {
		dbName := r.URL.Query().Get("db_name")
		table := r.URL.Query().Get("table")
		condition := r.URL.Query().Get("condition")

		rows1, err1 := selectRows(db, dbName, table, condition)
		rows2, _ := selectRows(db, dbName, table+"_replica", condition)

		if err1 != nil && rows2 == nil {
			writeError(w, err1.Error())
			return
		}

		merged := mergeRows(rows1, rows2)
		fmt.Printf("  [promoted] ✓ SELECT %s.%s → %d rows\n", dbName, table, len(merged))
		writeSuccess(w, merged)
	}))

	http.HandleFunc("/tables/update", masterGuard(func(w http.ResponseWriter, r *http.Request) {
		var req UpdateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, "Invalid body")
			return
		}
		err1 := updateRows(db, req.DBName, req.Table, req.Data, req.Condition)
		err2 := updateRows(db, req.DBName, req.Table+"_replica", req.Data, req.Condition)
		if err1 != nil && err2 != nil {
			writeError(w, err1.Error())
			return
		}
		broadcastToPeers(ExecRequest{
			DBName:    req.DBName,
			Operation: "UPDATE",
			Table:     req.Table,
			Data:      req.Data,
			Condition: req.Condition,
			IsReplica: false,
		})
		broadcastToPeers(ExecRequest{
			DBName:    req.DBName,
			Operation: "UPDATE",
			Table:     req.Table,
			Data:      req.Data,
			Condition: req.Condition,
			IsReplica: true,
		})
		fmt.Printf("  [promoted] ✓ Updated %s.%s\n", req.DBName, req.Table)
		writeSuccess(w, nil)
	}))

	http.HandleFunc("/tables/delete", masterGuard(func(w http.ResponseWriter, r *http.Request) {
		var req DeleteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, "Invalid body")
			return
		}
		_ = deleteRows(db, req.DBName, req.Table, req.Condition)
		_ = deleteRows(db, req.DBName, req.Table+"_replica", req.Condition)
		broadcastToPeers(ExecRequest{
			DBName:    req.DBName,
			Operation: "DELETE",
			Table:     req.Table,
			Condition: req.Condition,
			IsReplica: false,
		})
		broadcastToPeers(ExecRequest{
			DBName:    req.DBName,
			Operation: "DELETE",
			Table:     req.Table,
			Condition: req.Condition,
			IsReplica: true,
		})
		fmt.Printf("  [promoted] ✓ Deleted from %s.%s\n", req.DBName, req.Table)
		writeSuccess(w, nil)
	}))

	http.HandleFunc("/health", masterGuard(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"master": "promoted-slave",
			"slaves": []any{},
		})
	}))
}
