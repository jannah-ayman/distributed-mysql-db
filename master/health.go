package main

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

// slaveState holds the current known status of each slave
type slaveState struct {
	mu     sync.RWMutex
	status map[string]bool // url → online
}

func newSlaveState(urls []string) *slaveState {
	s := &slaveState{status: make(map[string]bool)}
	for _, url := range urls {
		s.status[url] = true // assume online at start
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

// checkSlave pings one slave and reports back via channel
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

// startHeartbeat pings all slaves every interval and updates their status
func startHeartbeat(slaves []string, state *slaveState, interval time.Duration) {
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

				// log transitions only
				if wasOnline && !s.Online {
					fmt.Printf("  ✗ Slave %s went OFFLINE\n", s.URL)
				} else if !wasOnline && s.Online {
					fmt.Printf("  ✓ Slave %s came back ONLINE\n", s.URL)
				}
			}

			time.Sleep(interval)
		}
	}()
}
