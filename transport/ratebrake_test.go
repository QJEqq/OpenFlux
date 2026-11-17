package transport

import (
	"context"
	"testing"
	"time"
)

func TestRateBrakeCutsHoldsAndRecovers(t *testing.T) {
	b := &RateBrake{}
	now := time.Now()
	for i := 0; i < 50; i++ { // 50 messages in the last second
		b.record(now.Add(-time.Duration(49-i) * 10 * time.Millisecond))
	}
	if rate, cut := b.Hit(now); !cut || rate != 35 { // 70 % of 50
		t.Fatalf("after a refusal: rate %v cut %v, want 35 true", rate, cut)
	}
	if !b.hold.Equal(now.Add(brakeHold)) {
		t.Fatalf("hold = %v, want %v", b.hold.Sub(now), brakeHold)
	}
	if rate, cut := b.Hit(now.Add(500 * time.Millisecond)); cut || rate != 35 {
		t.Fatalf("a refusal during the hold cut again: %v %v", rate, cut)
	}
	if rate, _ := b.Hit(now.Add(2 * time.Second)); rate > 0.7*35 {
		t.Fatalf("a second refusal did not cut below 70 %% of 35: %v", rate)
	}
}

func TestRateBrakePacesAfterAHit(t *testing.T) {
	b := &RateBrake{}
	b.Hit(time.Now()) // nothing sent before: the floor rate
	b.mu.Lock()
	b.hold = time.Now() // skip the hold for the test
	b.mu.Unlock()
	ctx := context.Background()
	began := time.Now()
	for i := 0; i < 4; i++ {
		if !b.Wait(ctx) {
			t.Fatal("Wait failed")
		}
	}
	// brakeMinRate = 2/s with a burst of 2: the last two wait ~0.5 s each.
	if took := time.Since(began); took < 700*time.Millisecond {
		t.Fatalf("4 messages at 2/s went in %v", took)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if b.Wait(cctx) {
		t.Fatal("Wait went on with its context done")
	}
}

func TestZeroRateBrakeDoesNotPace(t *testing.T) {
	var b RateBrake
	began := time.Now()
	for i := 0; i < 1000; i++ {
		b.Wait(context.Background())
	}
	if took := time.Since(began); took > 200*time.Millisecond {
		t.Fatalf("1000 unbraked messages took %v", took)
	}
}
