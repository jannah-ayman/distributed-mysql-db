package main

// ---- Requests the client sends to master ----

type CreateDBRequest struct {
	DBName string `json:"db_name"`
}

type DropDBRequest struct {
	DBName string `json:"db_name"`
}

type CreateTableRequest struct {
	DBName  string            `json:"db_name"`
	Table   string            `json:"table"`
	Columns map[string]string `json:"columns"`
}

type DropTableRequest struct {
	DBName string `json:"db_name"`
	Table  string `json:"table"`
}

type InsertRequest struct {
	DBName string         `json:"db_name"`
	Table  string         `json:"table"`
	Data   map[string]any `json:"data"`
}

type SelectRequest struct {
	DBName    string `json:"db_name"`
	Table     string `json:"table"`
	Condition string `json:"condition"`
}

type UpdateRequest struct {
	DBName    string         `json:"db_name"`
	Table     string         `json:"table"`
	Data      map[string]any `json:"data"`
	Condition string         `json:"condition"`
}

type DeleteRequest struct {
	DBName    string `json:"db_name"`
	Table     string `json:"table"`
	Condition string `json:"condition"`
}

// ---- What master sends to slaves (/internal/exec) ----

type ExecRequest struct {
	DBName    string            `json:"db_name"`
	Operation string            `json:"operation"`
	Table     string            `json:"table"`
	Columns   map[string]string `json:"columns"`
	Data      map[string]any    `json:"data"`
	Condition string            `json:"condition"`
	IsReplica bool              `json:"is_replica"`
}

// ---- What slaves return to master ----

type ExecResponse struct {
	Success bool             `json:"success"`
	Rows    []map[string]any `json:"rows"`
	Error   string           `json:"error"`
}

// ---- Shard metadata ----

// ShardInfo no longer stores ID ranges (Min/Max) since routing uses ID parity.
// DBName is stored so recovery sync knows which database the table lives in.
type ShardInfo struct {
	URL    string `json:"url"`
	DBName string `json:"db_name,omitempty"`
}

type Metadata struct {
	Shards map[string]map[string]ShardInfo `json:"shards"`
}

// ---- Health ----

type SlaveStatus struct {
	URL    string `json:"url"`
	Online bool   `json:"online"`
}

type HealthResponse struct {
	Master string        `json:"master"`
	Slaves []SlaveStatus `json:"slaves"`
}

// ---- Generic API response wrapper ----

type APIResponse struct {
	Success bool             `json:"success"`
	Message string           `json:"message,omitempty"`
	Rows    []map[string]any `json:"rows,omitempty"`
	Error   string           `json:"error,omitempty"`
}
