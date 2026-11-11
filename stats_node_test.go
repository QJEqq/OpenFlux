package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/p1neappleXpress/OpenFlux/transport"
	"github.com/p1neappleXpress/OpenFlux/transport/control"
	"github.com/p1neappleXpress/OpenFlux/transport/manager"
)

// statTransport is a carrier with counters the test sets.
type statTransport struct {
	jarTransport
	mu sync.Mutex
	st transport.TransportStats
}

func (s *statTransport) Stats() transport.TransportStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.st
}

func (s *statTransport) set(f func(*transport.TransportStats)) {
	s.mu.Lock()
	f(&s.st)
	s.mu.Unlock()
}

func TestCoreFeaturesHasExitStats(t *testing.T) {
	if !strings.Contains("\n"+coreFeatures, "\nexit-stats\n") {
		t.Fatal("--features lacks exit-stats")
	}
}

// The exit's stats: every carrier, highest priority first, with its own
// counters; no client yet before a handshake.
func TestExitStatsSnapshot(t *testing.T) {
	sess, err := transport.NewSession(transport.PeerParameters{Capabilities: control.CapabilityIPv4 | control.CapabilityTCP, MaxPacketSize: 1500}, true)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Stop()
	m := manager.New(sess, nil, "test-secret-long-enough", "ctx")
	boards := &statTransport{st: transport.TransportStats{BytesSent: 1200, BytesReceived: 3400, PacketsSent: 5, PacketsRecv: 7, Reconnects: 1, SendRetries: 2}}
	volga := &statTransport{st: transport.TransportStats{BytesSent: 10, BytesReceived: 20}}
	// As main.go does: each carrier into the session and into the manager.
	for _, c := range []struct {
		name string
		raw  transport.Transport
		prio int
	}{{"vyandex", volga, 50}, {"boards", boards, 70}} {
		if err := sess.AddTransport(c.name, c.raw, "test-secret-long-enough", "ctx", c.prio); err != nil {
			t.Fatal(err)
		}
		if err := m.Add(c.name, c.name, c.raw, c.prio, nil); err != nil {
			t.Fatal(err)
		}
	}

	st := snapshotExitStats(m, time.Unix(1000, 0), time.Unix(2000, 0))
	if st.Started != 1000 || st.At != 2000 || st.Client || st.Heard != 0 || st.Active != "" {
		t.Fatalf("snapshot %+v", st)
	}
	if len(st.Carriers) != 2 || st.Carriers[0].Name != "boards" || st.Carriers[1].Name != "vyandex" {
		t.Fatalf("carriers %+v", st.Carriers)
	}
	b := st.Carriers[0]
	if b.Priority != 70 || b.Sent != 1200 || b.Recv != 3400 || b.PktSent != 5 || b.PktRecv != 7 || b.Reconnects != 1 || b.Retries != 2 || b.Up {
		t.Fatalf("boards %+v", b)
	}

	// The writer keeps the file next to the cookie store current.
	old := exitStatsInterval
	exitStatsInterval = 30 * time.Millisecond
	t.Cleanup(func() { exitStatsInterval = old })
	path := filepath.Join(t.TempDir(), exitStatsFile)
	startExitStatsWriter(m, path)
	read := func(want func(exitStats) bool) exitStats {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for {
			var got exitStats
			if raw, err := os.ReadFile(path); err == nil && json.Unmarshal(raw, &got) == nil && want(got) {
				return got
			}
			if time.Now().After(deadline) {
				raw, _ := os.ReadFile(path)
				t.Fatalf("stats.json never got there: %s", raw)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	read(func(s exitStats) bool { return len(s.Carriers) == 2 && s.Carriers[0].Sent == 1200 })
	boards.set(func(s *transport.TransportStats) { s.BytesSent = 9999 })
	read(func(s exitStats) bool { return len(s.Carriers) == 2 && s.Carriers[0].Sent == 9999 })
}
