package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// slaveState holds the current known status of each slave.
type slaveState struct {
	mu     sync.RWMutex
	status map[string]bool
}

func newSlaveState(urls []string) *slaveState {
	s := &slaveState{status: make(map[string]bool)}
	for _, url := range urls {
		s.status[url] = true
	}
	return s
}

func (s *slaveState) setOnline(url string, online bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status[url] = online
}

func (s *slaveState) isOnline(url string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.status[url]
}

func (s *slaveState) allStatuses() []SlaveStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]SlaveStatus, 0, len(s.status))
	for url, online := range s.status {
		result = append(result, SlaveStatus{URL: url, Online: online})
	}
	return result
}

// checkSlave pings one slave and reports back via channel.
func checkSlave(url string, ch chan SlaveStatus) {
	client := http.Client{Timeout: 3 * time.Second}

	req, err := http.NewRequest("GET", url+"/ping", nil)
	if err != nil {
		ch <- SlaveStatus{URL: url, Online: false}
		return
	}
	req.Header.Set("X-Auth-Token", authToken)

	resp, err := client.Do(req)
	if err != nil {
		ch <- SlaveStatus{URL: url, Online: false}
		return
	}
	defer resp.Body.Close()

	ch <- SlaveStatus{URL: url, Online: resp.StatusCode == http.StatusOK}
}

// startHeartbeat pings all slaves every interval and updates their status.
//
// FIX (issue 2): when a slave transitions from offline → online, we now call
// syncSlaveOnRecovery to copy the missing rows from its _replica tables back
// into its primary tables, making it consistent again before it rejoins.
func startHeartbeat(slaves []string, state *slaveState, meta *Metadata, interval time.Duration) {
	go func() {
		for {
			ch := make(chan SlaveStatus, len(slaves))
			for _, url := range slaves {
				go checkSlave(url, ch)
			}

			for range slaves {
				s := <-ch
				wasOnline := state.isOnline(s.URL)
				state.setOnline(s.URL, s.Online)

				if wasOnline && !s.Online {
					fmt.Printf("  ✗ Slave %s went OFFLINE\n", s.URL)
				} else if !wasOnline && s.Online {
					fmt.Printf("  ✓ Slave %s came back ONLINE — starting recovery sync\n", s.URL)
					// Find a donor slave (any other online slave).
					for _, donor := range slaves {
						if donor != s.URL && state.isOnline(donor) {
							go syncSlaveOnRecovery(s.URL, donor, meta)
							break
						}
					}
					// Also push current metadata so it has the latest shard map.
					go func(url string) {
						syncMetadataToSlaves([]string{url}, state, meta)
					}(s.URL)
				}
			}

			time.Sleep(interval)
		}
	}()
}

// syncSlaveOnRecovery copies all rows that the recovered slave missed while it
// was offline. Strategy: for each table registered in metadata, ask the donor
// slave to SELECT from the _replica table (which holds rows that were primary
// on the recovered slave), then INSERT those rows into the recovered slave's
// primary table — overwriting by id to avoid duplicates.
//
// FIX (issue 2): this function did not exist before; nothing ran on recovery.
func syncSlaveOnRecovery(recoveredURL, donorURL string, meta *Metadata) {
	fmt.Printf("  → Recovery: copying missing data from %s to %s\n", donorURL, recoveredURL)

	tables := getAllTableNames(meta)
	if len(tables) == 0 {
		fmt.Printf("  → Recovery: no tables in metadata, nothing to sync\n")
		return
	}

	client := http.Client{Timeout: 30 * time.Second}

	for dbTable, dbName := range tables {
		// 1. Fetch the rows that lived on the recovered slave — they are stored
		//    in the donor's _replica table.
		fetchReq := ExecRequest{
			DBName:    dbName,
			Operation: "SELECT",
			Table:     dbTable,
			IsReplica: true, // read from donor's _replica (= recovered slave's data)
		}
		body, _ := json.Marshal(fetchReq)
		httpReq, err := http.NewRequest("POST", donorURL+"/internal/exec", bytes.NewReader(body))
		if err != nil {
			fmt.Printf("  ✗ Recovery SELECT build error for %s: %v\n", dbTable, err)
			continue
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("X-Auth-Token", authToken)

		resp, err := client.Do(httpReq)
		if err != nil {
			fmt.Printf("  ✗ Recovery SELECT error for %s: %v\n", dbTable, err)
			continue
		}

		var execResp ExecResponse
		if err := json.NewDecoder(resp.Body).Decode(&execResp); err != nil || !execResp.Success {
			resp.Body.Close()
			fmt.Printf("  ✗ Recovery SELECT decode error for %s\n", dbTable)
			continue
		}
		resp.Body.Close()

		rows := execResp.Rows
		fmt.Printf("  → Recovery: %d rows to replay into %s on %s\n", len(rows), dbTable, recoveredURL)

		// 2. For each row, DELETE the old version (by id) then re-INSERT on the
		//    recovered slave's primary table.  This handles the case where the
		//    slave had a stale row before it crashed.
		for _, row := range rows {
			id := fmt.Sprintf("%v", row["id"])

			// Delete stale copy if it exists.
			delReq := ExecRequest{
				DBName:    dbName,
				Operation: "DELETE",
				Table:     dbTable,
				Condition: "id = " + id,
				IsReplica: false,
			}
			sendExec(client, recoveredURL, delReq)

			// Re-insert the authoritative row.
			insReq := ExecRequest{
				DBName:    dbName,
				Operation: "INSERT",
				Table:     dbTable,
				Data:      row,
				IsReplica: false,
			}
			if err := sendExec(client, recoveredURL, insReq); err != nil {
				fmt.Printf("  ✗ Recovery INSERT id=%s into %s failed: %v\n", id, dbTable, err)
			}
		}

		fmt.Printf("  ✓ Recovery complete for table %s on %s\n", dbTable, recoveredURL)
	}
}

// sendExec is a small helper used only during recovery to avoid import cycles.
func sendExec(client http.Client, url string, req ExecRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequest("POST", url+"/internal/exec", bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Auth-Token", authToken)

	resp, err := client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var execResp ExecResponse
	if err := json.NewDecoder(resp.Body).Decode(&execResp); err != nil {
		return err
	}
	if !execResp.Success {
		return fmt.Errorf(execResp.Error)
	}
	return nil
}
