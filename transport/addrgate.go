package transport

import (
	"encoding/binary"
	"os"
	"sync"
	"time"
)

// AddressGate paces the requests that every process of this host makes to a
// service whose limit belongs to the address, not to a connection or a room:
// GCRA (a burst of Burst, then one request every Interval), with its state in a
// file all the processes share under an exclusive lock. A node runs one core
// per channel behind one address, so a budget kept per process would be spent
// as many times over as there are channels.
//
// The zero AddressGate, or one with Interval 0, does not pace. Without File
// (or where the file cannot be locked) the budget is this process's alone.
type AddressGate struct {
	Interval time.Duration
	Burst    int
	File     string

	mu  sync.Mutex
	tat time.Time // the theoretical arrival time: how far ahead the budget is spent
}

// Reserve takes the next request's turn and returns how long until it comes
// (0: at once). The turn is taken now, so callers that wait spread out instead
// of all going when the budget comes back.
func (g *AddressGate) Reserve(now time.Time) time.Duration {
	if g == nil || g.Interval <= 0 {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	tat := g.tat
	var f *os.File
	if g.File != "" {
		if fh, err := os.OpenFile(g.File, os.O_RDWR|os.O_CREATE, 0o666); err == nil {
			if lockFile(fh) == nil {
				f = fh
				defer func() {
					_ = unlockFile(f)
					_ = f.Close()
				}()
				var b [8]byte
				if n, _ := f.ReadAt(b[:], 0); n == 8 {
					if shared := time.Unix(0, int64(binary.BigEndian.Uint64(b[:]))); shared.After(tat) {
						tat = shared
					}
				}
			} else {
				_ = fh.Close()
			}
		}
	}
	if tat.Before(now) {
		tat = now
	}
	burst := max(g.Burst, 1)
	wait := tat.Sub(now) - time.Duration(burst-1)*g.Interval
	g.tat = tat.Add(g.Interval)
	if f != nil {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(g.tat.UnixNano()))
		_, _ = f.WriteAt(b[:], 0)
	}
	return max(wait, 0)
}
