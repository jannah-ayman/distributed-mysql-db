package main

import (
	"fmt"
	"strings"
	"sync"
)

var insertCounter int
var insertMu sync.Mutex

// routeInsert uses round-robin to alternate inserts between slaves.
func routeInsert(slaves []string, state *slaveState) (primary string, replica string, err error) {
	var online []string
	for _, url := range slaves {
		if state.isOnline(url) {
			online = append(online, url)
		}
	}

	if len(online) == 0 {
		return "", "", fmt.Errorf("no online slaves available")
	}

	insertMu.Lock()
	idx := insertCounter % len(online)
	insertCounter++
	insertMu.Unlock()

	primary = online[idx]

	for _, url := range online {
		if url != primary {
			replica = url
			break
		}
	}

	return primary, replica, nil
}

// routeSelectByID returns the slave that owns a specific ID.
// Falls back to the other slave's _replica table if the primary is offline.
func routeSelectByID(meta *Metadata, table string, id int, slaves []string, state *slaveState) (string, bool, error) {
	shard, ok := getShardForID(meta, table, id)
	if !ok {
		// No shard metadata — fall back to full scan on any online slave.
		for _, url := range slaves {
			if state.isOnline(url) {
				return url, false, nil
			}
		}
		return "", false, fmt.Errorf("no shard found for table %s id %d and no slaves online", table, id)
	}

	// Primary owner is online — use it directly.
	if state.isOnline(shard.URL) {
		return shard.URL, false, nil
	}

	// Primary is offline — use the surviving slave's _replica table.
	for _, url := range slaves {
		if url != shard.URL && state.isOnline(url) {
			fmt.Printf("  ⚠ Primary for id %d is offline, routing SELECT to replica at %s\n", id, url)
			return url, true, nil // true = useReplica
		}
	}

	return "", false, fmt.Errorf("all slaves offline")
}

// routeSelectAll returns query targets for a full table scan.
//
//   - All slaves online → each slave serves its own primary table.
//   - A slave is offline → the surviving slave serves both its primary table
//     AND its _replica table (which holds the offline slave's data).
//
// FIX (issue 1): previously this function always set useReplica=false,
// so the offline slave's rows were never retrieved.
func routeSelectAll(slaves []string, state *slaveState) []selectTarget {
	allOnline := true
	for _, url := range slaves {
		if !state.isOnline(url) {
			allOnline = false
			break
		}
	}

	var targets []selectTarget

	if allOnline {
		for _, url := range slaves {
			targets = append(targets, selectTarget{url: url, useReplica: false})
		}
		return targets
	}

	// At least one slave is offline.
	// Surviving slaves serve both primary and replica tables.
	for _, url := range slaves {
		if state.isOnline(url) {
			targets = append(targets, selectTarget{url: url, useReplica: false})
			targets = append(targets, selectTarget{url: url, useReplica: true})
		}
	}
	return targets
}

// selectTarget describes one query to send during a full table scan.
type selectTarget struct {
	url        string
	useReplica bool
}

// mergeRows combines rows from multiple result sets, deduplicating by id.
func mergeRows(allRows ...[]map[string]any) []map[string]any {
	seen := make(map[string]bool)
	var merged []map[string]any

	for _, rows := range allRows {
		for _, row := range rows {
			id := fmt.Sprintf("%v", row["id"])
			if !seen[id] {
				seen[id] = true
				merged = append(merged, row)
			}
		}
	}

	return merged
}

// extractIDFromCondition tries to parse a simple "id = N" or "id=N" condition.
// Returns (id, true) on success, (0, false) otherwise.
//
// FIX (issue 6): previously returned -1 on failure and the call site checked
// `> 0`, which would incorrectly skip id=0 (not a real MySQL id, but still
// wrong) and would pass id=-1 through as a valid value after a bad parse.
func extractIDFromCondition(condition string) (int, bool) {
	// Normalise: remove spaces around '='
	s := strings.ReplaceAll(condition, " ", "")

	var id int
	if _, err := fmt.Sscanf(s, "id=%d", &id); err == nil {
		return id, true
	}
	return 0, false
}
