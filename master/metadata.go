package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

const metadataFile = "metadata.json"

var metaMu sync.RWMutex // protects meta

// loadMetadata loads metadata from disk, or returns a fresh empty one
func loadMetadata() *Metadata {
	data, err := os.ReadFile(metadataFile)
	if err != nil {
		// no file yet — start fresh
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

// saveMetadata persists current metadata to disk
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

// registerTable adds shard info for a new table
// splits the ID range (1..1000) evenly across however many slaves are online
func registerTable(m *Metadata, table string, onlineSlaves []string) {
	metaMu.Lock()
	defer metaMu.Unlock()

	count := len(onlineSlaves)
	rangeSize := 1000 / count

	shards := make(map[string]ShardInfo)
	for i, url := range onlineSlaves {
		min := i*rangeSize + 1
		max := (i + 1) * rangeSize
		if i == count-1 {
			max = 1000 // last shard gets the remainder
		}
		shards[fmt.Sprintf("shard_%d", i+1)] = ShardInfo{
			URL: url,
			Min: min,
			Max: max,
		}
	}

	m.Shards[table] = shards
}

// getShardForID returns the shard that owns the given primary key ID
func getShardForID(m *Metadata, table string, id int) (ShardInfo, bool) {
	metaMu.RLock()
	defer metaMu.RUnlock()

	shards, ok := m.Shards[table]
	if !ok {
		return ShardInfo{}, false
	}

	for _, shard := range shards {
		if id >= shard.Min && id <= shard.Max {
			return shard, true
		}
	}

	return ShardInfo{}, false
}

// getAllShards returns all shards for a given table
func getAllShards(m *Metadata, table string) []ShardInfo {
	metaMu.RLock()
	defer metaMu.RUnlock()

	shards, ok := m.Shards[table]
	if !ok {
		return nil
	}

	result := make([]ShardInfo, 0, len(shards))
	for _, s := range shards {
		result = append(result, s)
	}
	return result
}

// removeTable removes a table's shard info from metadata
func removeTable(m *Metadata, table string) {
	metaMu.Lock()
	defer metaMu.Unlock()
	delete(m.Shards, table)
}
