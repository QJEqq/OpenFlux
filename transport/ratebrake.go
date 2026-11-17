package transport

import (
	"context"
	"sync"
	"time"
)

// RateBrake paces the messages a carrier sends through somebody else's
// service once that service has refused one for coming too often. Such limits
// belong to the address, not to a room or a socket: live (2026-11-17)
// cups.online refused every room of an address within 50 ms of the others
// (429), and Avito three rooms at once (ERR_ACTION_RATE_LIMITED). Unpaced,
// the refused messages come back as retransmissions that only keep the limit
// up (38 % of all cups messages); backing off by doubling pauses starves the
// carrier under a limit that is a steady rate. So, on a refusal: everyone
// holds for brakeHold and the rate drops to 70 % of what was being sent
// (multiplicative decrease); after brakeCalm without one it grows by a
// message a second each second (additive increase), and at brakeOffRate the
// pacing stops. The zero RateBrake does not pace until its first Hit.
type RateBrake struct {
	mu        sync.Mutex
	rate      float64 // messages a second; 0: not pacing
	tokens    float64
	last      time.Time // tokens were last added
	hold      time.Time // no message before this
	lastHit   time.Time
	lastRaise time.Time
	sent      []time.Time // sends within the last second, oldest first
}

const (
	brakeHold       = time.Second
	brakeMinRate    = 2.0
	brakeCalm       = 3 * time.Second
	brakeRaiseEvery = time.Second
	brakeOffRate    = 200.0
)

// Hit takes a refusal. It returns the rate now kept and whether this refusal
// cut it (false for one of the same burst, during the hold).
func (b *RateBrake) Hit(now time.Time) (rate float64, cut bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if now.Before(b.hold) {
		return b.rate, false
	}
	b.prune(now)
	r := 0.7 * float64(len(b.sent))
	if b.rate > 0 {
		r = min(r, 0.7*b.rate)
	}
	b.rate = max(r, brakeMinRate)
	b.tokens = 0
	b.last = now.Add(brakeHold)
	b.hold = now.Add(brakeHold)
	b.lastHit, b.lastRaise = now, now
	return b.rate, true
}

// Rate is the rate kept now, 0 when not pacing.
func (b *RateBrake) Rate() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.rate
}

// Wait blocks until a message may go; false when ctx ended first.
func (b *RateBrake) Wait(ctx context.Context) bool {
	for {
		b.mu.Lock()
		now := time.Now()
		var d time.Duration
		switch {
		case now.Before(b.hold):
			d = b.hold.Sub(now)
		case b.rate == 0:
			b.record(now)
			b.mu.Unlock()
			return ctx.Err() == nil
		default:
			if now.Sub(b.lastHit) >= brakeCalm && now.Sub(b.lastRaise) >= brakeRaiseEvery {
				b.rate++
				b.lastRaise = now
				if b.rate >= brakeOffRate {
					b.rate = 0
					b.record(now)
					b.mu.Unlock()
					return ctx.Err() == nil
				}
			}
			burst := max(2, b.rate/2)
			b.tokens = min(burst, b.tokens+now.Sub(b.last).Seconds()*b.rate)
			b.last = now
			if b.tokens >= 1 {
				b.tokens--
				b.record(now)
				b.mu.Unlock()
				return ctx.Err() == nil
			}
			d = time.Duration((1 - b.tokens) / b.rate * float64(time.Second))
		}
		b.mu.Unlock()
		t := time.NewTimer(d)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return false
		}
	}
}

// record notes a send, for the rate a refusal cuts down from. Called with b.mu.
func (b *RateBrake) record(now time.Time) {
	b.prune(now)
	b.sent = append(b.sent, now)
}

func (b *RateBrake) prune(now time.Time) {
	i := 0
	for i < len(b.sent) && now.Sub(b.sent[i]) > time.Second {
		i++
	}
	b.sent = b.sent[i:]
}
