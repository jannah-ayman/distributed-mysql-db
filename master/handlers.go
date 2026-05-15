package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

// authenticate hashes the incoming token and compares against the stored hash.
func authenticate(r *http.Request) bool {
	incoming := r.Header.Get("X-Auth-Token")
	incomingHash := fmt.Sprintf("%x", sha256.Sum256([]byte(incoming)))
	return incomingHash == authTokenHash
}

// ---- /db/create ----

func handleCreateDB(slaves []string, state *slaveState) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !authenticate(r) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req CreateDBRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, "Invalid body: "+err.Error())
			return
		}

		errs := broadcastToAll(slaves, state, ExecRequest{
			DBName:    req.DBName,
			Operation: "CREATE_DB",
		})

		if len(errs) == countOnline(slaves, state) {
			writeError(w, "All online slaves failed to create DB")
			return
		}

		fmt.Printf("✓ Created DB %s\n", req.DBName)
		writeSuccess(w, "Database created", nil)
	}
}

// ---- /db/drop ----

func handleDropDB(slaves []string, state *slaveState) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !authenticate(r) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodDelete {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req DropDBRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, "Invalid body: "+err.Error())
			return
		}

		errs := broadcastToAll(slaves, state, ExecRequest{
			DBName:    req.DBName,
			Operation: "DROP_DB",
		})

		if len(errs) == countOnline(slaves, state) {
			writeError(w, "All online slaves failed to drop DB")
			return
		}

		fmt.Printf("✓ Dropped DB %s\n", req.DBName)
		writeSuccess(w, "Database dropped", nil)
	}
}

// ---- /tables/create ----

func handleCreateTable(meta *Metadata, slaves []string, state *slaveState) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !authenticate(r) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req CreateTableRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, "Invalid body: "+err.Error())
			return
		}

		errs := broadcastToAll(slaves, state, ExecRequest{
			DBName:    req.DBName,
			Operation: "CREATE_TABLE",
			Table:     req.Table,
			Columns:   req.Columns,
		})

		if len(errs) == countOnline(slaves, state) {
			writeError(w, "All online slaves failed to create table")
			return
		}

		onlineSlaves := onlineList(slaves, state)
		if len(onlineSlaves) > 0 {
			registerTable(meta, req.Table, onlineSlaves)
		}
		saveMetadata(meta)
		syncMetadataToSlaves(slaves, state, meta)

		fmt.Printf("✓ Created table %s.%s\n", req.DBName, req.Table)
		writeSuccess(w, "Table created", nil)
	}
}

// ---- /tables/drop ----

func handleDropTable(meta *Metadata, slaves []string, state *slaveState) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !authenticate(r) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodDelete {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req DropTableRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, "Invalid body: "+err.Error())
			return
		}

		errs := broadcastToAll(slaves, state, ExecRequest{
			DBName:    req.DBName,
			Operation: "DROP_TABLE",
			Table:     req.Table,
		})

		if len(errs) == countOnline(slaves, state) {
			writeError(w, "All online slaves failed to drop table")
			return
		}

		removeTable(meta, req.Table)
		saveMetadata(meta)
		syncMetadataToSlaves(slaves, state, meta)

		fmt.Printf("✓ Dropped table %s.%s\n", req.DBName, req.Table)
		writeSuccess(w, "Table dropped", nil)
	}
}

// ---- /tables/insert ----

func handleInsert(slaves []string, state *slaveState) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !authenticate(r) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req InsertRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, "Invalid body: "+err.Error())
			return
		}

		primaryURL, replicaURL, err := routeInsert(slaves, state)
		if err != nil {
			writeError(w, "Routing failed: "+err.Error())
			return
		}

		resp, err := sendToSlave(primaryURL, ExecRequest{
			DBName:    req.DBName,
			Operation: "INSERT",
			Table:     req.Table,
			Data:      req.Data,
			IsReplica: false,
		})
		if err != nil || !resp.Success {
			msg := "Primary INSERT failed"
			if resp != nil {
				msg = resp.Error
			}
			writeError(w, msg)
			return
		}

		if replicaURL != "" {
			go func() {
				resp, err := sendToSlave(replicaURL, ExecRequest{
					DBName:    req.DBName,
					Operation: "INSERT",
					Table:     req.Table,
					Data:      req.Data,
					IsReplica: true,
				})
				if err != nil || (resp != nil && !resp.Success) {
					fmt.Printf("  ✗ Replica INSERT to %s failed\n", replicaURL)
				} else {
					fmt.Printf("  ✓ Replica INSERT to %s succeeded\n", replicaURL)
				}
			}()
		}

		fmt.Printf("✓ Inserted into %s.%s via %s\n", req.DBName, req.Table, primaryURL)
		writeSuccess(w, "Row inserted", nil)
	}
}

// ---- /tables/select ----

// FIX (issue 1): routeSelectByID now returns a useReplica flag so that when
// a slave is offline its rows are fetched from the surviving slave's _replica
// table. routeSelectAll was also fixed in router.go.
func handleSelect(meta *Metadata, slaves []string, state *slaveState) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !authenticate(r) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		dbName := r.URL.Query().Get("db_name")
		table := r.URL.Query().Get("table")
		condition := r.URL.Query().Get("condition")

		// Fast path: "id = N" — route to exactly one shard.
		if id, ok := extractIDFromCondition(condition); ok {
			url, useReplica, err := routeSelectByID(meta, table, id, slaves, state)
			if err != nil {
				writeError(w, err.Error())
				return
			}
			resp, err := sendToSlave(url, ExecRequest{
				DBName:    dbName,
				Operation: "SELECT",
				Table:     table,
				Condition: condition,
				IsReplica: useReplica,
			})
			if err != nil || !resp.Success {
				writeError(w, "SELECT failed")
				return
			}
			writeSuccess(w, "ok", resp.Rows)
			return
		}

		// Full scan — query all targets (primary + replicas when a slave is down).
		targets := routeSelectAll(slaves, state)
		if len(targets) == 0 {
			writeError(w, "No slaves online")
			return
		}

		type result struct {
			rows []map[string]any
			err  error
		}

		ch := make(chan result, len(targets))

		for _, t := range targets {
			go func(target selectTarget) {
				resp, err := sendToSlave(target.url, ExecRequest{
					DBName:    dbName,
					Operation: "SELECT",
					Table:     table,
					Condition: condition,
					IsReplica: target.useReplica,
				})
				if err != nil || !resp.Success {
					ch <- result{err: fmt.Errorf("slave %s failed", target.url)}
					return
				}
				ch <- result{rows: resp.Rows}
			}(t)
		}

		var allRows [][]map[string]any
		for range targets {
			r := <-ch
			if r.err == nil {
				allRows = append(allRows, r.rows)
			}
		}

		merged := mergeRows(allRows...)
		fmt.Printf("✓ SELECT %s.%s → %d rows (merged from %d targets)\n", dbName, table, len(merged), len(targets))
		writeSuccess(w, "ok", merged)
	}
}

// ---- /tables/update ----

func handleUpdate(slaves []string, state *slaveState) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !authenticate(r) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodPut {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req UpdateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, "Invalid body: "+err.Error())
			return
		}

		var wg sync.WaitGroup
		var mu sync.Mutex
		var errs []string

		for _, url := range slaves {
			if !state.isOnline(url) {
				continue
			}
			wg.Add(1)
			go func(u string) {
				defer wg.Done()

				resp, err := sendToSlave(u, ExecRequest{
					DBName:    req.DBName,
					Operation: "UPDATE",
					Table:     req.Table,
					Data:      req.Data,
					Condition: req.Condition,
					IsReplica: false,
				})
				if err != nil || !resp.Success {
					mu.Lock()
					errs = append(errs, fmt.Sprintf("%s primary: %v", u, err))
					mu.Unlock()
				}

				resp, err = sendToSlave(u, ExecRequest{
					DBName:    req.DBName,
					Operation: "UPDATE",
					Table:     req.Table,
					Data:      req.Data,
					Condition: req.Condition,
					IsReplica: true,
				})
				if err != nil || !resp.Success {
					mu.Lock()
					errs = append(errs, fmt.Sprintf("%s replica: %v", u, err))
					mu.Unlock()
				}
			}(url)
		}
		wg.Wait()

		if len(errs) > 0 {
			fmt.Printf("  ⚠ Some UPDATE errors: %v\n", errs)
		}

		fmt.Printf("✓ Updated %s.%s WHERE %s\n", req.DBName, req.Table, req.Condition)
		writeSuccess(w, "Row updated", nil)
	}
}

// ---- /tables/delete ----

func handleDelete(slaves []string, state *slaveState) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !authenticate(r) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodDelete {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req DeleteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, "Invalid body: "+err.Error())
			return
		}

		var wg sync.WaitGroup
		for _, url := range slaves {
			if !state.isOnline(url) {
				continue
			}
			wg.Add(1)
			go func(u string) {
				defer wg.Done()
				sendToSlave(u, ExecRequest{
					DBName:    req.DBName,
					Operation: "DELETE",
					Table:     req.Table,
					Condition: req.Condition,
					IsReplica: false,
				})
				sendToSlave(u, ExecRequest{
					DBName:    req.DBName,
					Operation: "DELETE",
					Table:     req.Table,
					Condition: req.Condition,
					IsReplica: true,
				})
			}(url)
		}
		wg.Wait()

		fmt.Printf("✓ Deleted from %s.%s WHERE %s\n", req.DBName, req.Table, req.Condition)
		writeSuccess(w, "Row deleted", nil)
	}
}

// ---- /health ----

func handleHealth(state *slaveState) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !authenticate(r) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(HealthResponse{
			Master: "online",
			Slaves: state.allStatuses(),
		})
	}
}

// ---- helpers ----

func writeSuccess(w http.ResponseWriter, message string, rows []map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(APIResponse{
		Success: true,
		Message: message,
		Rows:    rows,
	})
}

func writeError(w http.ResponseWriter, errMsg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusInternalServerError)
	json.NewEncoder(w).Encode(APIResponse{
		Success: false,
		Error:   errMsg,
	})
}

// countOnline returns the number of currently online slaves.
// FIX (issue 10): used instead of len(slaves) to distinguish "all online
// slaves failed" from "all slaves (including offline ones) failed".
func countOnline(slaves []string, state *slaveState) int {
	n := 0
	for _, url := range slaves {
		if state.isOnline(url) {
			n++
		}
	}
	return n
}

// onlineList returns URLs of all currently online slaves.
func onlineList(slaves []string, state *slaveState) []string {
	var out []string
	for _, url := range slaves {
		if state.isOnline(url) {
			out = append(out, url)
		}
	}
	return out
}
