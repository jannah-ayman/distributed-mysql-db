package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
)

// authenticate hashes the incoming token and compares against stored hash
func authenticate(r *http.Request) bool {
	incoming := r.Header.Get("X-Auth-Token")
	incomingHash := fmt.Sprintf("%x", sha256.Sum256([]byte(incoming)))
	return incomingHash == authTokenHash
}

// pingHandler lets master check if this slave is alive
func pingHandler(w http.ResponseWriter, r *http.Request) {

	if !authenticate(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("pong"))
}

// execHandler is the main handler — master sends all operations here
func execHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		if !authenticate(r) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req ExecRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, "Invalid request body: "+err.Error())
			return
		}

		// if IsReplica, operate on the _replica version of the table
		table := req.Table
		if req.IsReplica {
			table = req.Table + "_replica"
		}

		switch req.Operation {

		case "CREATE_DB":
			if err := createDatabase(db, req.DBName); err != nil {
				writeError(w, "CREATE_DB failed: "+err.Error())
				return
			}
			fmt.Printf("  ✓ Created database %s\n", req.DBName)
			writeSuccess(w, nil)

		case "DROP_DB":
			if err := dropDatabase(db, req.DBName); err != nil {
				writeError(w, "DROP_DB failed: "+err.Error())
				return
			}
			fmt.Printf("  ✓ Dropped database %s\n", req.DBName)
			writeSuccess(w, nil)

		case "CREATE_TABLE":
			// create both the primary table and the replica table
			if err := createTable(db, req.DBName, table, req.Columns); err != nil {
				writeError(w, "CREATE_TABLE failed: "+err.Error())
				return
			}
			// also create the _replica version if this is a primary table creation
			if !req.IsReplica {
				replicaTable := req.Table + "_replica"
				if err := createTable(db, req.DBName, replicaTable, req.Columns); err != nil {
					writeError(w, "CREATE_TABLE replica failed: "+err.Error())
					return
				}
			}
			fmt.Printf("  ✓ Created table %s.%s\n", req.DBName, table)
			writeSuccess(w, nil)

		case "DROP_TABLE":
			if err := dropTable(db, req.DBName, table); err != nil {
				writeError(w, "DROP_TABLE failed: "+err.Error())
				return
			}
			// also drop the replica table
			if !req.IsReplica {
				replicaTable := req.Table + "_replica"
				if err := dropTable(db, req.DBName, replicaTable); err != nil {
					writeError(w, "DROP_TABLE replica failed: "+err.Error())
					return
				}
			}
			fmt.Printf("  ✓ Dropped table %s.%s\n", req.DBName, table)
			writeSuccess(w, nil)

		case "INSERT":
			if err := insertRow(db, req.DBName, table, req.Data); err != nil {
				writeError(w, "INSERT failed: "+err.Error())
				return
			}
			fmt.Printf("  ✓ Inserted into %s.%s\n", req.DBName, table)
			writeSuccess(w, nil)
		case "UPSERT":
			if err := upsertRow(db, req.DBName, table, req.Data); err != nil {
				writeError(w, "UPSERT failed: "+err.Error())
				return
			}
			fmt.Printf("  ✓ Upserted into %s.%s\n", req.DBName, table)
			writeSuccess(w, nil)

		case "SELECT":
			rows, err := selectRows(db, req.DBName, table, req.Condition)
			if err != nil {
				writeError(w, "SELECT failed: "+err.Error())
				return
			}
			fmt.Printf("  ✓ SELECT from %s.%s → %d rows\n", req.DBName, table, len(rows))
			writeSuccess(w, rows)

		case "UPDATE":
			if err := updateRows(db, req.DBName, table, req.Data, req.Condition); err != nil {
				writeError(w, "UPDATE failed: "+err.Error())
				return
			}
			fmt.Printf("  ✓ Updated %s.%s WHERE %s\n", req.DBName, table, req.Condition)
			writeSuccess(w, nil)

		case "DELETE":
			if err := deleteRows(db, req.DBName, table, req.Condition); err != nil {
				writeError(w, "DELETE failed: "+err.Error())
				return
			}
			fmt.Printf("  ✓ Deleted from %s.%s WHERE %s\n", req.DBName, table, req.Condition)
			writeSuccess(w, nil)

		default:
			writeError(w, "Unknown operation: "+req.Operation)
		}
	}
}

// metadataHandler returns the stored metadata copy (used during master failover)
func metadataHandler(localMeta *Metadata) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		if !authenticate(r) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(localMeta)
	}
}

// syncMetadataHandler lets master push updated metadata to this slave
func syncMetadataHandler(localMeta *Metadata) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

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

		*localMeta = incoming
		fmt.Println("  ✓ Metadata synced from master")
		w.WriteHeader(http.StatusOK)
	}
}

// ---- helpers ----

func writeSuccess(w http.ResponseWriter, rows []map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(ExecResponse{
		Success: true,
		Rows:    rows,
	})
}

func writeError(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusInternalServerError)
	json.NewEncoder(w).Encode(ExecResponse{
		Success: false,
		Error:   msg,
	})
}
