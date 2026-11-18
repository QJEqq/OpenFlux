package transport

import (
	"path/filepath"
	"testing"
	"time"
)

func TestAddressGateTakesABurstThenPaces(t *testing.T) {
	g := &AddressGate{Interval: 10 * time.Millisecond, Burst: 5}
	now := time.Now()
	for i := 0; i < 5; i++ {
		if d := g.Reserve(now); d != 0 {
			t.Fatalf("request %d of the burst waits %v", i+1, d)
		}
	}
	for i := 1; i <= 3; i++ {
		if d := g.Reserve(now); d != time.Duration(i)*10*time.Millisecond {
			t.Fatalf("request %d after the burst waits %v, want %v", 5+i, d, time.Duration(i)*10*time.Millisecond)
		}
	}
	// A second of quiet gives the burst back, no more.
	later := now.Add(time.Second)
	for i := 0; i < 5; i++ {
		if d := g.Reserve(later); d != 0 {
			t.Fatalf("after a quiet second, request %d waits %v", i+1, d)
		}
	}
	if d := g.Reserve(later); d == 0 {
		t.Fatal("the quiet second gave back more than the burst")
	}
}

// Two gates on one file are the two processes of one host: they share one budget.
func TestAddressGateIsSharedThroughItsFile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "gate")
	a := &AddressGate{Interval: 100 * time.Millisecond, Burst: 2, File: file}
	b := &AddressGate{Interval: 100 * time.Millisecond, Burst: 2, File: file}
	now := time.Now()
	if a.Reserve(now) != 0 || b.Reserve(now) != 0 {
		t.Fatal("the shared burst of two did not let two requests through")
	}
	if d := a.Reserve(now); d != 100*time.Millisecond {
		t.Fatalf("the third request of the host waits %v, want 100ms", d)
	}
	if d := b.Reserve(now); d != 200*time.Millisecond {
		t.Fatalf("the fourth request of the host waits %v, want 200ms", d)
	}
}

func TestZeroAddressGateDoesNotPace(t *testing.T) {
	var g AddressGate
	now := time.Now()
	for i := 0; i < 100; i++ {
		if d := g.Reserve(now); d != 0 {
			t.Fatalf("the zero gate paced request %d by %v", i+1, d)
		}
	}
	var none *AddressGate
	if none.Reserve(now) != 0 {
		t.Fatal("a nil gate paced")
	}
}
