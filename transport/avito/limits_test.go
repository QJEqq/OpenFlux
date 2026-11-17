package avito

import (
	"sync/atomic"
	"testing"
	"time"
)

func noReconnects(t *testing.T, peers ...*peer) {
	t.Helper()
	for _, p := range peers {
		for _, ws := range p.allWS() {
			if n := ws.stats.reconnects.Load(); n != 0 {
				t.Fatalf("room %s reconnected %d times", ws.roomID[:8], n)
			}
		}
	}
}

// Avito closes the socket (1009) on an op over its size; the ops stay under
// it, so a stream goes through without a room dropping.
func TestOpsStayUnderAvitosMessageLimit(t *testing.T) {
	f := newFakeAvito(t, 0)
	f.setReadLimit(200 << 10) // 128 KB of data is 175 KB of base64
	exit, client := startStripedPair(t, fastConfig())
	const n = 40
	var got atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		expectInOrder(t, client, n, 30*time.Second, &got)
	}()
	sendWindowed(t, exit, n, n, &got, done) // all in flight: the lanes fill whole ops
	<-done
	noReconnects(t, exit, client)
}

// An op refused as too many waits for the pause and goes again: everything
// arrives in order and no room is torn down for it.
func TestRateLimitedOpsWaitAndGoAgain(t *testing.T) {
	f := newFakeAvito(t, 0)
	f.setOpLimit(15, 4)
	exit, client := startStripedPair(t, fastConfig())
	const n = 30
	var got atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		expectInOrder(t, client, n, 60*time.Second, &got)
	}()
	sendWindowed(t, exit, n, 8, &got, done)
	<-done
	f.mu.Lock()
	limited := f.rateLimited
	f.mu.Unlock()
	if limited == 0 {
		t.Fatal("the fake's op limit never bit: the test proves nothing")
	}
	noReconnects(t, exit, client)
}

// An op never goes over ChunkBytes: Avito closes the socket (1009) on one
// over its size. A piece that does not fit waits for the next op, nothing is
// lost or reordered, and a piece bigger than an op goes alone.
func TestFillOpKeepsTheOpUnderItsSize(t *testing.T) {
	const max = 128 << 10
	next := 0
	src := func(int) [][]byte {
		p := make([]byte, 32<<10)
		p[0] = byte(next)
		next++
		return [][]byte{p}
	}
	var carry [][]byte
	seen := 0
	for op := 0; op < 5; op++ {
		var blob []byte
		blob, carry = fillOp(nil, carry, src, max)
		if len(blob) > max {
			t.Fatalf("op %d is %d bytes, over %d", op, len(blob), max)
		}
		splitFramed(blob, func(p []byte) {
			if int(p[0]) != seen%256 {
				t.Fatalf("piece %d came as %d", seen, p[0])
			}
			seen++
		})
	}
	if seen < 15 { // 3 pieces of 32 KB (+ 4-byte frames) fit 128 KB
		t.Fatalf("only %d pieces in 5 ops", seen)
	}
	big := [][]byte{make([]byte, 200<<10)}
	blob, rest := fillOp(nil, big, func(int) [][]byte { return nil }, max)
	if len(blob) != 4+200<<10 || len(rest) != 0 {
		t.Fatalf("a piece bigger than an op: blob %d, %d left", len(blob), len(rest))
	}
}

// A room Avito takes no more ops in (ERR_ROOM_OPERATION_LIMIT) rests instead
// of reconnecting at once, over and over, each time spending a handshake of
// the address's ten a minute.
func TestRoomRestsAfterItsOperationLimit(t *testing.T) {
	f := newFakeAvito(t, 0)
	cfg := fastConfig()
	cfg.RoomRest, cfg.RoomRestMax = time.Hour, time.Hour
	exit, client := startStripedPair(t, cfg)
	f.setRoomOpLimit(4)
	for i := 0; i < 40; i++ {
		_ = exit.Send(seqFrame(i))
	}
	time.Sleep(2 * time.Second)
	rested := 0
	for _, p := range []*peer{exit, client} {
		for _, ws := range p.allWS() {
			switch n := ws.stats.reconnects.Load(); {
			case n > 1:
				t.Fatalf("room %s reconnected %d times instead of resting", ws.roomID[:8], n)
			case n == 1:
				rested++
			}
		}
	}
	if rested == 0 {
		t.Fatal("no room hit the operation limit: the test proves nothing")
	}
}
