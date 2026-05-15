package main

import (
	"fmt"
	"strconv"
	"sync"
)

var insertCounter int
var insertMu sync.Mutex

// routeInsert uses round-robin to alternate inserts between slaves
func routeInsert(slaves []string, state *slaveState) (primary string, replica string, err error) {

	// collect online slaves in order
	var online []string
	for _, url := range slaves {
		if state.isOnline(url) {
			online = append(online, url)
		}
	}

	if len(online) == 0 {
		return "", "", fmt.Errorf("no online slaves available")
	}

	// round-robin: alternate which slave is primary each insert
	insertMu.Lock()
	idx := insertCounter % len(online)
	insertCounter++
	insertMu.Unlock()

	primary = online[idx]

	// replica = the other slave
	for _, url := range online {
		if url != primary {
			replica = url
			break
		}
	}

	return primary, replica, nil
}

// routeSelectByID returns the slave that owns a specific ID
// falls back to replica slave if primary is offline
func routeSelectByID(meta *Metadata, table string, id int, slaves []string, state *slaveState) (string, error) {

	shard, ok := getShardForID(meta, table, id)
	if !ok {
		return "", fmt.Errorf("no shard found for table %s id %d", table, id)
	}

	// primary owner is online
	if state.isOnline(shard.URL) {
		return shard.URL, nil
	}

	// primary is offline → use the other slave's replica
	for _, url := range slaves {
		if url != shard.URL && state.isOnline(url) {
			fmt.Printf("  ⚠ Primary for id %d is offline, routing to replica at %s\n", id, url)
			return url, nil
		}
	}

	return "", fmt.Errorf("all slaves offline")
}

// routeSelectAll returns query targets for a full table scan.
// - if all slaves are online: query each slave's primary table
// - if a slave is offline: query the surviving slave's primary table AND
//   its _replica table (which holds the offline slave's data), then merge
func routeSelectAll(slaves []string, state *slaveState) []selectTarget {
	type onlineInfo struct {
		url    string
		online bool
	}

	info := make([]onlineInfo, len(slaves))
	for i, url := range slaves {
		info[i] = onlineInfo{url: url, online: state.isOnline(url)}
	}

	allOnline := true
	for _, s := range info {
		if !s.online {
			allOnline = false
			break
		}
	}

	var targets []selectTarget

	if allOnline {
		// normal case: each slave serves its own primary table
		for _, s := range info {
			targets = append(targets, selectTarget{url: s.url, useReplica: false})
		}
		return targets
	}

	// at least one slave is offline
	// surviving slave serves both its primary AND the replica of the dead slave
	for _, s := range info {
		if s.online {
			targets = append(targets, selectTarget{url: s.url, useReplica: false})
			targets = append(targets, selectTarget{url: s.url, useReplica: true})
		}
	}
	return targets
}

// selectTarget describes one query to send during a full table scan
type selectTarget struct {
	url        string
	useReplica bool // if true, query the _replica table instead of primary
}

// mergeRows combines rows from multiple slaves, removing duplicates by id
func mergeRows(allRows ...[]map[string]any) []map[string]any {
	seen := make(map[string]bool)
	var merged []map[string]any

	for _, rows := range allRows {
		for _, row := range rows {
			// use id as dedup key
			id := fmt.Sprintf("%v", row["id"])
			if !seen[id] {
				seen[id] = true
				merged = append(merged, row)
			}
		}
	}

	return merged
}

// extractIDFromCondition tries to parse a simple "id = N" condition
// returns -1 if it can't parse it (meaning we need a full scan)
func extractIDFromCondition(condition string) int {
	var id int
	_, err := fmt.Sscanf(condition, "id = %d", &id)
	if err == nil {
		return id
	}
	// try "id=N"
	n, err := strconv.Atoi(condition)
	if err == nil {
		return n
	}
	return -1
}