package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// sendToSlave sends an ExecRequest to a slave's /internal/exec endpoint
func sendToSlave(slaveURL string, req ExecRequest) (*ExecResponse, error) {

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal error: %w", err)
	}

	httpReq, err := http.NewRequest("POST", slaveURL+"/internal/exec", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("request build error: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Auth-Token", authToken)

	client := http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("send error: %w", err)
	}
	defer resp.Body.Close()

	var result ExecResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode error: %w", err)
	}

	return &result, nil
}

// broadcastToAll sends the same ExecRequest to every *online* slave.
// Used for DDL operations (CREATE DB, DROP DB, CREATE TABLE, DROP TABLE).
//
// FIX (#7): Previously, offline slaves were sent to the channel as errors,
// which caused every DDL call to fail whenever any slave was down. That
// defeats the point of fault tolerance. Now we simply skip offline slaves
// and only report actual network/response errors from slaves we did reach.
// The caller (handleCreateDB etc.) only gets errors if an online slave fails.
func broadcastToAll(slaves []string, state *slaveState, req ExecRequest) []error {
	type result struct {
		url string
		err error
	}

	// Only attempt to contact online slaves.
	var targets []string
	for _, url := range slaves {
		if state.isOnline(url) {
			targets = append(targets, url)
		} else {
			fmt.Printf("  ⚠ Skipping offline slave %s for DDL broadcast\n", url)
		}
	}

	if len(targets) == 0 {
		return []error{fmt.Errorf("no online slaves to broadcast to")}
	}

	ch := make(chan result, len(targets))

	for _, url := range targets {
		go func(u string) {
			resp, err := sendToSlave(u, req)
			if err != nil {
				ch <- result{url: u, err: err}
				return
			}
			if !resp.Success {
				ch <- result{url: u, err: errors.New(resp.Error)}
				return
			}
			ch <- result{url: u, err: nil}
		}(url)
	}

	var errs []error
	for range targets {
		r := <-ch
		if r.err != nil {
			fmt.Printf("  ✗ Broadcast to %s failed: %v\n", r.url, r.err)
			errs = append(errs, r.err)
		} else {
			fmt.Printf("  ✓ Broadcast to %s succeeded\n", r.url)
		}
	}

	return errs
}

// syncMetadataToSlaves pushes the current metadata to all online slaves
func syncMetadataToSlaves(slaves []string, state *slaveState, meta *Metadata) {
	body, err := json.Marshal(meta)
	if err != nil {
		fmt.Println("✗ Could not marshal metadata for sync:", err)
		return
	}

	for _, url := range slaves {
		if !state.isOnline(url) {
			continue
		}
		go func(u string) {
			req, _ := http.NewRequest("POST", u+"/internal/sync-metadata", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Auth-Token", authToken)

			client := http.Client{Timeout: 5 * time.Second}
			resp, err := client.Do(req)
			if err != nil {
				fmt.Printf("  ✗ Metadata sync to %s failed: %v\n", u, err)
				return
			}
			defer resp.Body.Close()
			fmt.Printf("  ✓ Metadata synced to %s\n", u)
		}(url)
	}
}
