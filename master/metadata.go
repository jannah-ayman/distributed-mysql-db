package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

const metadataFile = "metadata.json"

var metaMu sync.RWMutex

// loadMetadata loads metadata from disk, or returns a fresh empty one.
func loadMetadata() *Metadata {
	data, err := os.ReadFile(metadataFile)
	if err != nil {
		return &Metadata{Shards: make(map[string]map[string]ShardInfo)}
	}

	var m Metadata
	if err := json.Unmarshal(data, &m); err != nil {
		fmt.Println("✗ Could not parse metadata file, starting fresh:", err)
		return &Metadata{Shards: make(map[string]map[string]ShardInfo)}
	}

	fmt.Println("✓ Loaded metadata from disk")
	return &m
}

// saveMetadata persists current metadata to disk.
func saveMetadata(m *Metadata) {
	metaMu.RLock()
	defer metaMu.RUnlock()

	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		fmt.Println("✗ Could not marshal metadata:", err)
		return
	}

	if err := os.WriteFile(metadataFile, data, 0644); err != nil {
		fmt.Println("✗ Could not save metadata:", err)
	}
}

// registerTable records which slaves hold a table and stores the DB name.
//
// FIX (sharding mismatch + missing DBName): The old code split a 1–1000 ID
// range across slaves, but inserts use round-robin and MySQL auto_increment
// assigns IDs by parity. The shard map is now only used to store the DB name
// and slave list — ID-based routing uses parity directly (see router.go).
//
// FIX (DBName): dbName is now stored in every ShardInfo so recovery sync
// knows which database a table belongs to without a separate lookup.
func registerTable(m *Metadata, table, dbName string, onlineSlaves []string) {
	metaMu.Lock()
	defer metaMu.Unlock()

	shards := make(map[string]ShardInfo)
	for i, url := range onlineSlaves {
		shards[fmt.Sprintf("shard_%d", i+1)] = ShardInfo{
			URL:    url,
			DBName: dbName,
		}
	}

	m.Shards[table] = shards
}

// getSlavesForTable returns the slave URLs registered for a table.
func getSlavesForTable(m *Metadata, table string) []string {
	metaMu.RLock()
	defer metaMu.RUnlock()

	shards, ok := m.Shards[table]
	if !ok {
		return nil
	}
	urls := make([]string, 0, len(shards))
	for _, s := range shards {
		urls = append(urls, s.URL)
	}
	return urls
}

// removeTable removes a table's shard info from metadata.
func removeTable(m *Metadata, table string) {
	metaMu.Lock()
	defer metaMu.Unlock()
	delete(m.Shards, table)
}

// getAllTableNames returns a map of tableName → dbName for every table
// registered in the metadata. Used by recovery sync.
func getAllTableNames(m *Metadata) map[string]string {
	metaMu.RLock()
	defer metaMu.RUnlock()

	result := make(map[string]string, len(m.Shards))
	for table, shards := range m.Shards {
		dbName := ""
		for _, s := range shards {
			if s.DBName != "" {
				dbName = s.DBName
				break
			}
		}
		result[table] = dbName
	}
	return result
}
