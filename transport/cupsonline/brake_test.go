package cupsonline

import (
	"sync/atomic"
	"testing"
	"time"
)

// cups.online refuses an address's messages over its budget with 429, for
// all of its rooms at once. Braked, the rooms stay near the budget and the
// refusals (each one a retransmission) are a fraction of the unbraked ones;
// either way every frame arrives once and in order.
func TestBrakeKeepsTheRoomsUnderTheAddressLimit(t *testing.T) {
	const n = 40
	run := func(t *testing.T, brake bool) (refused, relayed int) {
		f := newFakeCups(t)
		f.locked(func() { f.limitRate, f.limitBurst = 20, 10 })
		cfg := fastConfig()
		cfg.RateBrake = brake
		exit, client := startStripedPair(t, cfg)
		var got atomic.Int64
		done := make(chan struct{})
		go func() {
			defer close(done)
			expectInOrderCounting(t, client, n, 60*time.Second, &got)
		}()
		sendWindowed(t, exit, n, &got, done)
		<-done
		f.locked(func() { refused, relayed = f.refused, f.relayed })
		return
	}
	var without, with [2]int
	t.Run("unbraked", func(t *testing.T) { a, b := run(t, false); without = [2]int{a, b} })
	t.Run("braked", func(t *testing.T) { a, b := run(t, true); with = [2]int{a, b} })
	t.Logf("429s / messages relayed: unbraked %d / %d, braked %d / %d", without[0], without[1], with[0], with[1])
	if without[0] == 0 {
		t.Fatal("the fake's limit never refused anything: the test proves nothing")
	}
	if with[0]*3 > without[0] {
		t.Fatalf("braked: %d refusals, unbraked %d: want at most a third", with[0], without[0])
	}
}

func TestIsRateLimited(t *testing.T) {
	if !isRateLimited(map[string]interface{}{"code": 429.0}) || isRateLimited(map[string]interface{}{"code": 400.0}) || isRateLimited("x") {
		t.Fatal("isRateLimited")
	}
}
