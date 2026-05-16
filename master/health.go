package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"
)

// slaveState holds the current known status of each slave.
type slaveState struct {
	mu     sync.RWMutex
	status map[string]bool
}

// FIX (master recovery): initialize all slaves as OFFLINE (false) instead of
// online (true). This way the very first heartbeat tick that finds them alive
// transitions them from offline→online and triggers syncSlaveOnRecovery for
// each one. Without this, when the master restarts after a downtime all slaves
// appear to have been "online the whole time" so no recovery sync is ever
// kicked off, and the slaves that missed writes while the master was down are
// never brought up to date.
func newSlaveState(urls []string) *slaveState {
	s := &slaveState{status: make(map[string]bool)}
	for _, url := range urls {
		s.status[url] = false // start as offline; first heartbeat will correct this
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
	sort.Slice(result, func(i, j int) bool {
		return result[i].URL < result[j].URL
	})
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
					// FIX (master recovery): this branch now also fires on the
					// very first heartbeat tick (because newSlaveState sets all
					// slaves to offline), so recovery sync runs automatically
					// whenever the master starts up and finds its slaves alive.
					fmt.Printf("  ✓ Slave %s came back ONLINE — starting recovery sync\n", s.URL)
					for _, donor := range slaves {
						if donor != s.URL && state.isOnline(donor) {
							go syncSlaveOnRecovery(s.URL, donor, meta)
							break
						}
					}
					go func(url string) {
						syncMetadataToSlaves([]string{url}, state, meta)
					}(s.URL)
				}
			}

			time.Sleep(interval)
		}
	}()
}

// syncSlaveOnRecovery copies all rows the recovered slave missed while offline.
func syncSlaveOnRecovery(recoveredURL, donorURL string, meta *Metadata) {
	fmt.Printf("  → Recovery: copying missing data from %s to %s\n", donorURL, recoveredURL)

	tables := getAllTableNames(meta)
	if len(tables) == 0 {
		fmt.Printf("  → Recovery: no tables in metadata, nothing to sync\n")
		return
	}

	client := http.Client{Timeout: 30 * time.Second}

	for dbTable, dbName := range tables {
		fetchReq := ExecRequest{
			DBName:    dbName,
			Operation: "SELECT",
			Table:     dbTable,
			IsReplica: false,
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

		for _, row := range rows {
			upsertReq := ExecRequest{
				DBName:    dbName,
				Operation: "UPSERT",
				Table:     dbTable,
				Data:      row,
				IsReplica: true,
			}
			if _, err := sendExec(client, recoveredURL, upsertReq); err != nil {
				fmt.Printf("  ✗ Recovery UPSERT id=%v into %s failed: %v\n", row["id"], dbTable, err)
			}
		}

		fmt.Printf("  ✓ Recovery complete for table %s on %s\n", dbTable, recoveredURL)
	}
}

// sendExec sends an ExecRequest and returns (success bool, error).
func sendExec(client http.Client, url string, req ExecRequest) (bool, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return false, err
	}
	httpReq, err := http.NewRequest("POST", url+"/internal/exec", bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Auth-Token", authToken)

	resp, err := client.Do(httpReq)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	var execResp ExecResponse
	if err := json.NewDecoder(resp.Body).Decode(&execResp); err != nil {
		return false, err
	}
	if !execResp.Success {
		return false, errors.New(execResp.Error)
	}
	return true, nil
}
