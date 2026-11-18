package transport

import (
	"sync/atomic"
	"testing"
)

func (w *startCountingWire) SendBlocked() bool { return w.blocked.Load() }

// A carrier the peer is still heard on but that cannot send for now (its
// service refuses this side: cups.online blocks an address for ~20 s) steps
// aside for the next heard carrier, and takes the traffic back when it can.
func TestSessionRoutesAroundABlockedCarrier(t *testing.T) {
	client, exit, cw, _ := linkedSessions(t, "cupsonline", "avito")
	var got atomic.Int32
	exit.Receive(func([]byte) { got.Add(1) })
	startPair(t, client, exit)

	send := func(n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			if err := client.Send(testIPv4(40, 6)); err != nil {
				t.Fatal(err)
			}
		}
	}
	cw["cupsonline"].blocked.Store(true)
	before := sentCount(cw["avito"])
	want := got.Load() + 5
	send(5)
	eventually(t, "data over the carrier that is not blocked", func() bool { return got.Load() >= want })
	if sentCount(cw["avito"]) == before {
		t.Fatal("the blocked carrier kept the traffic")
	}

	cw["cupsonline"].blocked.Store(false)
	before = sentCount(cw["avito"])
	want = got.Load() + 5
	send(5)
	eventually(t, "data back on the preferred carrier", func() bool { return got.Load() >= want })
	if after := sentCount(cw["avito"]); after != before {
		t.Fatalf("the lower carrier still carried data after the block (%d -> %d)", before, after)
	}
}

// With nothing else heard, a blocked carrier still carries: refusing would be
// no better.
func TestSessionKeepsABlockedCarrierWhenItIsTheOnlyOne(t *testing.T) {
	client, exit, cw, _ := linkedSessions(t, "cupsonline")
	var got atomic.Int32
	exit.Receive(func([]byte) { got.Add(1) })
	startPair(t, client, exit)
	cw["cupsonline"].blocked.Store(true)
	if err := client.Send(testIPv4(40, 6)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "data over the only carrier", func() bool { return got.Load() >= 1 })
}
