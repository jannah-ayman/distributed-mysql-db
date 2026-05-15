package main

import (
	"fmt"
	"strings"
	"sync"
)

var insertCounter int
var insertMu sync.Mutex

// routeInsert uses round-robin to pick a primary slave, replica is any other online slave.
//
// FIX (#3): We reset insertCounter to 0 whenever the number of online slaves
// changes. Without this, the counter keeps its old value across topology changes.
// Example: counter=5, 2 slaves online → idx=1 (fine). One slave drops → 1 slave
// online, counter=5 → idx=5%1=0 (still fine, but counter keeps growing). Now
// slave comes back → 2 slaves online, counter=6 → idx=0 again, so slave[0] gets
// every insert until counter happens to be odd. Resetting on change means the
// first insert after any topology change always starts cleanly at index 0.
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
	// Reset counter when the pool size has changed since the last insert.
	// lastOnlineCount is tracked below so we can detect the change.
	if len(online) != lastOnlineCount {
		insertCounter = 0
		lastOnlineCount = len(online)
	}
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

// lastOnlineCount remembers how many slaves were online during the previous
// insert, so routeInsert can detect topology changes and reset the counter.
var lastOnlineCount int

// routeSelectByID routes a single-ID lookup using modulo so it scales to any
// number of slaves and stays consistent with auto_increment_offset assignments.
//
// MySQL slaves are started with auto_increment_offset = (index+1) and
// auto_increment_increment = len(slaves), so slave[i] owns IDs where
// (id - 1) % len(slaves) == i.  If the owner is offline we fall back to the
// surviving slave's _replica table.
func routeSelectByID(slaves []string, state *slaveState, id int) (string, bool, error) {
	if len(slaves) == 0 {
		return "", false, fmt.Errorf("no slaves configured")
	}

	ownerIdx := (id - 1) % len(slaves)
	ownerURL := slaves[ownerIdx]

	if state.isOnline(ownerURL) {
		return ownerURL, false, nil
	}

	// Owner offline — read from a surviving slave's _replica table.
	for _, url := range slaves {
		if url != ownerURL && state.isOnline(url) {
			fmt.Printf("  ⚠ Owner for id=%d offline, reading replica at %s\n", id, url)
			return url, true, nil
		}
	}
	return "", false, fmt.Errorf("all slaves offline")
}

// routeSelectAll returns the targets for a full table scan.
// When all slaves are up each serves its own primary table.
// When a slave is down the surviving slaves serve both primary and _replica.
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

	for _, url := range slaves {
		if state.isOnline(url) {
			targets = append(targets, selectTarget{url: url, useReplica: false})
			targets = append(targets, selectTarget{url: url, useReplica: true})
		}
	}
	return targets
}

type selectTarget struct {
	url        string
	useReplica bool
}

// mergeRows deduplicates rows from multiple shards by id.
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

func extractIDFromCondition(condition string) (int, bool) {
	s := strings.ReplaceAll(condition, " ", "")
	var id int
	if _, err := fmt.Sscanf(s, "id=%d", &id); err == nil {
		return id, true
	}
	return 0, false
}
