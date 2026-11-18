package cupsonline

import (
	"path/filepath"
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

// The address's budget is a window (live: ~2500 RPCs in two minutes, then every
// one refused until the window moves on). The gate keeps the rooms inside it
// ahead of time: no refusals, where the rooms unpaced spend it and are refused.
func TestAddressGateKeepsTheRoomsInsideTheWindow(t *testing.T) {
	const n = 60
	run := func(t *testing.T, gated bool) (refused int) {
		f := newFakeCups(t)
		f.locked(func() { f.limitN, f.limitWindow = 40, 2*time.Second }) // 20 a second, in 2 s windows
		cfg := fastConfig()
		cfg.RateBrake = false
		if gated {
			cfg.AddrRate, cfg.AddrBurst, cfg.AddrGateFile = 15, 8, filepath.Join(t.TempDir(), "gate")
		}
		exit, client := startStripedPair(t, cfg)
		var got atomic.Int64
		done := make(chan struct{})
		go func() {
			defer close(done)
			expectInOrderCounting(t, client, n, 90*time.Second, &got)
		}()
		sendWindowed(t, exit, n, &got, done)
		<-done
		f.locked(func() { refused = f.refused })
		return
	}
	var without, with int
	t.Run("ungated", func(t *testing.T) { without = run(t, false) })
	t.Run("gated", func(t *testing.T) { with = run(t, true) })
	t.Logf("429s: ungated %d, gated %d", without, with)
	if without == 0 {
		t.Fatal("the fake's window never refused anything: the test proves nothing")
	}
	if with != 0 {
		t.Fatalf("gated: %d refusals, want none", with)
	}
}

// While cups.online refuses the address, the transport says it cannot send, so
// the session routes over another carrier; the first message taken lifts it.
func TestRefusalsBlockSendingUntilAMessageIsTaken(t *testing.T) {
	f := newFakeCups(t)
	cfg := fastConfig()
	cfg.RateBrake = false
	exit, client := startStripedPair(t, cfg)
	if exit.SendBlocked() {
		t.Fatal("blocked before anything was refused")
	}
	f.locked(func() { f.limitN, f.limitWindow = 1, time.Hour }) // one more message, then refusals
	for i := 0; i < 5; i++ {
		_ = exit.Send(seqFrame(i))
	}
	waitFor(t, 5*time.Second, "the refusals to block sending", exit.SendBlocked)
	f.locked(func() { f.limitN = 0 })
	_ = exit.Send(seqFrame(5))
	waitFor(t, 10*time.Second, "a taken message to lift the block", func() bool { return !exit.SendBlocked() })
	_ = client
}
