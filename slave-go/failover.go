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
// but ONLY if no other peer slave has already promoted, to prevent split-brain.
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
			fails = 0
		}
	}
}

// anotherSlaveIsActingAsMaster checks all known peer slaves to see if any
// has already promoted itself. This prevents split-brain.
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

func sendExecHTTP(url string, req ExecRequest) (*ExecResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	client := http.Client{Timeout: 5 * time.Second}
	httpReq, err := http.NewRequest("POST", url+"/internal/exec", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Auth-Token", authToken)
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var result ExecResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
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

	http.HandleFunc("/tables/insert", masterGuard(func(w http.ResponseWriter, r *http.Request) {
		/*
		 * FIX (promoted insert round-robin):
		 *
		 * When a slave is promoted it is the SOLE master — there is no other
		 * master to share the "primary" role with. The correct behaviour is:
		 *
		 *   1. Always INSERT the row into THIS slave's primary table so MySQL
		 *      assigns a deterministic ID via auto_increment_offset.
		 *   2. Broadcast that row (with its generated ID) as an UPSERT to every
		 *      peer slave's _replica table for fault-tolerance.
		 *
		 * The old counter-based "alternate primary between self and peer" scheme
		 * was wrong because:
		 *   a) The peer is not a promoted master; making it own primary rows
		 *      breaks the auto_increment_offset contract.
		 *   b) The SELECT-after-INSERT used to retrieve the peer-generated ID
		 *      had a TOCTOU race: a concurrent insert could make us replicate
		 *      the wrong row.
		 */
		var req InsertRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, "Invalid body")
			return
		}

		// Step 1: insert locally, capture the auto-assigned ID.
		generatedID, err := insertRowReturnID(db, req.DBName, req.Table, req.Data)
		if err != nil {
			writeError(w, err.Error())
			return
		}

		fmt.Printf("  [promoted] ✓ Inserted into %s.%s id=%v (self=primary)\n",
			req.DBName, req.Table, generatedID)

		// Step 2: replicate to all peer _replica tables with the exact same ID.
		dataWithID := make(map[string]any, len(req.Data)+1)
		for k, v := range req.Data {
			dataWithID[k] = v
		}
		dataWithID["id"] = generatedID

		broadcastToPeers(ExecRequest{
			DBName:    req.DBName,
			Operation: "UPSERT",
			Table:     req.Table,
			Data:      dataWithID,
			IsReplica: true,
		})

		writeSuccess(w, nil)
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
