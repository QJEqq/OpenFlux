package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/p1neappleXpress/OpenFlux/transport/manager"
	"github.com/p1neappleXpress/OpenFlux/utils"
)

// exitStatsFile is written next to the cookie store: what the exit's carriers
// are doing, for the node agent (ofnode) to show in the augmenter's panel.
// The counters are the core's own and start from zero with each run; the
// agent adds runs up.
const exitStatsFile = "stats.json"

// exitStatsInterval is how often the exit rewrites it (a var for tests).
var exitStatsInterval = 20 * time.Second

type exitStats struct {
	At      int64  `json:"at"`      // when written (unix)
	Started int64  `json:"started"` // when this run of the core started
	Mode    string `json:"mode"`    // "session", "classic" or "" (no client yet)
	Client  bool   `json:"client"`  // a client is connected now
	Active  string `json:"active"`  // the carrier the traffic rides now
	// Heard is when the client was last heard on any carrier (0: never).
	Heard    int64          `json:"heard"`
	Carriers []carrierStats `json:"carriers"`
}

type carrierStats struct {
	Name       string `json:"name"`
	Priority   int    `json:"priority"`
	Up         bool   `json:"up"`
	Heard      int64  `json:"heard"`
	Sent       uint64 `json:"sent"` // bytes
	Recv       uint64 `json:"recv"`
	PktSent    uint64 `json:"pkt_sent"`
	PktRecv    uint64 `json:"pkt_recv"`
	Reconnects uint64 `json:"reconnects"`
	Retries    uint64 `json:"retries"`
}

func unixOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

// snapshotExitStats reads the exit's session and carriers.
func snapshotExitStats(m *manager.Manager, started, now time.Time) exitStats {
	sess := m.Session()
	st := exitStats{At: now.Unix(), Started: started.Unix(), Mode: sess.Mode(), Client: sess.IsConnected(),
		Active: sess.ActiveTransport(), Carriers: []carrierStats{}}
	for _, c := range sess.CarrierStates() {
		cs := carrierStats{Name: c.Name, Priority: c.Priority, Up: c.Up, Heard: unixOrZero(c.Heard)}
		if raw := m.Raw(c.Name); raw != nil {
			ts := raw.Stats()
			cs.Sent, cs.Recv, cs.PktSent, cs.PktRecv = ts.BytesSent, ts.BytesReceived, ts.PacketsSent, ts.PacketsRecv
			cs.Reconnects, cs.Retries = ts.Reconnects, ts.SendRetries
		}
		if cs.Heard > st.Heard {
			st.Heard = cs.Heard
		}
		st.Carriers = append(st.Carriers, cs)
	}
	return st
}

// startExitStatsWriter keeps stats.json next to the cookie store current.
func startExitStatsWriter(m *manager.Manager, path string) {
	started := time.Now()
	utils.SafeGo("exit-stats", func() {
		ticker := time.NewTicker(exitStatsInterval)
		defer ticker.Stop()
		for {
			if err := writeExitStats(path, snapshotExitStats(m, started, time.Now())); err != nil {
				utils.Debugf("[STATS] %v", err)
			}
			<-ticker.C
		}
	})
}

func writeExitStats(path string, st exitStats) error {
	raw, err := json.Marshal(st)
	if err != nil {
		return err
	}
	tmp := filepath.Join(filepath.Dir(path), "."+filepath.Base(path)+".tmp")
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
