package main

// ---- What master sends to slaves (/internal/exec) ----

type ExecRequest struct {
	DBName    string            `json:"db_name"`
	Operation string            `json:"operation"`   // CREATE_DB, DROP_DB, CREATE_TABLE, DROP_TABLE, INSERT, SELECT, UPDATE, DELETE
	Table     string            `json:"table"`
	Columns   map[string]string `json:"columns"`     // for CREATE_TABLE only
	Data      map[string]any    `json:"data"`
	Condition string            `json:"condition"`
	IsReplica bool              `json:"is_replica"`  // true = operate on _replica table
}

// ---- What slave returns ----

type ExecResponse struct {
	Success bool             `json:"success"`
	Rows    []map[string]any `json:"rows"`
	Error   string           `json:"error"`
}

// ---- Metadata copy slave keeps locally ----

type ShardInfo struct {
	URL string `json:"url"`
	Min int    `json:"min"`
	Max int    `json:"max"`
}

type Metadata struct {
	Shards map[string]map[string]ShardInfo `json:"shards"`
}
