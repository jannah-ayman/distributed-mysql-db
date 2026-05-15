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
	Columns map[string]string `json:"columns"` // column name → MySQL type e.g. "age": "INT"
}

type DropTableRequest struct {
	DBName string `json:"db_name"`
	Table  string `json:"table"`
}

type InsertRequest struct {
	DBName string         `json:"db_name"`
	Table  string         `json:"table"`
	Data   map[string]any `json:"data"` // column → value
}

type SelectRequest struct {
	DBName    string `json:"db_name"`
	Table     string `json:"table"`
	Condition string `json:"condition"` // optional WHERE clause, e.g. "age > 20"
}

type UpdateRequest struct {
	DBName    string         `json:"db_name"`
	Table     string         `json:"table"`
	Data      map[string]any `json:"data"`      // columns to update
	Condition string         `json:"condition"` // WHERE clause
}

type DeleteRequest struct {
	DBName    string `json:"db_name"`
	Table     string `json:"table"`
	Condition string `json:"condition"` // WHERE clause
}

// ---- What master sends to slaves (/internal/exec) ----

type ExecRequest struct {
	DBName    string         `json:"db_name"`
	Operation string         `json:"operation"`  // CREATE_DB, DROP_DB, CREATE_TABLE, DROP_TABLE, INSERT, SELECT, UPDATE, DELETE
	Table     string         `json:"table"`
	Columns   map[string]string `json:"columns"` // for CREATE_TABLE
	Data      map[string]any `json:"data"`
	Condition string         `json:"condition"`
	IsReplica bool           `json:"is_replica"` // true = write to _replica table instead
}

// ---- What slaves return to master ----

type ExecResponse struct {
	Success bool             `json:"success"`
	Rows    []map[string]any `json:"rows"`
	Error   string           `json:"error"`
}

// ---- Shard metadata ----

type ShardInfo struct {
	URL string `json:"url"`
	Min int    `json:"min"` // min primary key this shard owns
	Max int    `json:"max"` // max primary key this shard owns
}

// key: table name → shard index (1 or 2) → ShardInfo
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
