package main

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

var isMaster int32 // 0 = slave, 1 = acting as master

func watchMaster(masterURL string, meta *Metadata) {
	client := http.Client{Timeout: 3 * time.Second}
	fails := 0
	for {
		time.Sleep(5 * time.Second)
		req, _ := http.NewRequest("GET", masterURL+"/health", nil)
		req.Header.Set("X-Auth-Token", authToken)
		resp, err := client.Do(req)
		if err != nil || resp.StatusCode != http.StatusOK {
			fails++
			fmt.Printf("  ⚠ Master unreachable (%d/3)\n", fails)
			if fails >= 3 {
				promote()
			}
		} else {
			fails = 0
			atomic.StoreInt32(&isMaster, 0)
			resp.Body.Close()
		}
	}
}

func promote() {
	if atomic.CompareAndSwapInt32(&isMaster, 0, 1) {
		fmt.Println("  ★ Master appears down — this slave is now acting as master")
		// Register /promote so the GUI can be pointed here
	}
}
