package main

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

var isMaster int32 // 0 = normal slave, 1 = acting as master

// watchMaster pings the real master every 5 s. After 3 consecutive failures
// (~15 s) this slave promotes itself by setting isMaster = 1.
//
// The slave doesn't try to re-register HTTP routes — the GUI discovers the
// new master by polling /ping on each known node and switching to whichever
// one responds. The slave's existing /internal/exec routes keep working
// for any traffic that arrives; it just won't serve the GUI's /db/* or
// /tables/* routes (those are master-only). For a uni project this is the
// right trade-off: keep it simple, document the limitation.
func watchMaster(masterURL string) {
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
	}
}
