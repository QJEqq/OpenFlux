package avito

import (
	"testing"
	"time"

	"github.com/p1neappleXpress/OpenFlux/transport"
)

// Avito closes a socket that carries nothing for 60 s. Pinged, an idle room
// stays open: no reconnect, so no handshake spent from the 10-a-minute budget.
func TestIdleRoomsStayOpenWithPings(t *testing.T) {
	f := newFakeAvito(t, 0)
	f.setIdle(300 * time.Millisecond)
	cfg := fastConfig()
	cfg.PingInterval = 80 * time.Millisecond
	exit, client := startStripedPair(t, cfg)
	time.Sleep(1500 * time.Millisecond) // five idle periods with nothing to send
	for _, p := range []*peer{exit, client} {
		for _, ws := range p.allWS() {
			if n := ws.stats.reconnects.Load(); n != 0 {
				t.Fatalf("room %s reconnected %d times while idle", ws.roomID[:8], n)
			}
		}
	}
}

// The control: without pings the fake's idle close does hit the rooms.
func TestIdleRoomsDropWithoutPings(t *testing.T) {
	f := newFakeAvito(t, 0)
	f.setIdle(300 * time.Millisecond)
	cfg := fastConfig()
	cfg.PingInterval = 0
	exit := mustStart(t, "", false, cfg) // its rooms come up, then sit idle
	waitFor(t, 5*time.Second, "an idle room to be dropped", func() bool {
		for _, ws := range exit.allWS() {
			if ws.stats.reconnects.Load() > 0 {
				return true
			}
		}
		return false
	})
}

// With the address's budget a burst of four and then nothing, the four rooms
// that come up make two whole lanes - a send and a receive room each - so a
// cold client can already hear the exit.
func TestRoomsComeUpInWholeLanes(t *testing.T) {
	newFakeAvito(t, 0)
	cfg := fastConfig()
	cfg.NumRooms = 4
	exit := mustStart(t, "", false, cfg)
	waitFor(t, 5*time.Second, "the exit's rooms", func() bool { return allConnected(exit) })
	tr := NewAvitoTransport(exit.RoomList(), transport.DefaultConfig(), "", true)
	tr.config = cfg
	tr.gate = &dialGate{refill: time.Hour, burst: 4, spacing: time.Millisecond}
	t.Cleanup(func() { tr.Stop() })
	if err := tr.Start(); err != nil {
		t.Fatal(err)
	}
	up := func() (n int) {
		for _, ws := range tr.allWS() {
			if ws.connected.Load() {
				n++
			}
		}
		return n
	}
	waitFor(t, 3*time.Second, "the burst of four rooms", func() bool { return up() == 4 })
	time.Sleep(200 * time.Millisecond)
	if n := up(); n != 4 {
		t.Fatalf("%d rooms up past the burst", n)
	}
	for i := 0; i < 2; i++ {
		if !tr.txWss[i].connected.Load() || !tr.rxWss[i].connected.Load() {
			t.Fatalf("lane %d is not whole: send room up %v, receive room up %v",
				i, tr.txWss[i].connected.Load(), tr.rxWss[i].connected.Load())
		}
	}
}
