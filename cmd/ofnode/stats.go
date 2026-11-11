package main

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Per-channel statistics for the augmenter's panel, gathered every statsEvery:
//   - stats.json — the channel's core writes it next to cookies.json (carriers,
//     bytes, the carrier in use, when the client was last heard);
//   - signin-state — the exit's documents whose sign-in is gone ("login") or
//     whose carrier is down, and since when;
//   - the core process's CPU and resident memory (/proc, Linux);
//   - totals.json — traffic summed over the core's runs: the core's own counters
//     start from zero with each run, the agent adds the runs up.

const (
	statsFile  = "stats.json"
	signinFile = "signin-state"
	totalsFile = "totals.json"
	clkTck     = 100 // USER_HZ: /proc CPU ticks per second
	tailLines  = 200 // core output lines kept per channel (the panel's channel log)
)

type carrierSnap struct {
	Name       string `json:"name"`
	Priority   int    `json:"priority"`
	Up         bool   `json:"up"`
	Heard      int64  `json:"heard"`
	Sent       uint64 `json:"sent"`
	Recv       uint64 `json:"recv"`
	PktSent    uint64 `json:"pkt_sent"`
	PktRecv    uint64 `json:"pkt_recv"`
	Reconnects uint64 `json:"reconnects"`
	Retries    uint64 `json:"retries"`
	TotalSent  uint64 `json:"total_sent"` // over all runs (totals.json)
	TotalRecv  uint64 `json:"total_recv"`
}

type chanStats struct {
	At        int64         `json:"at"`
	Started   int64         `json:"started"`
	Mode      string        `json:"mode"`
	Client    bool          `json:"client"`
	Active    string        `json:"active"`
	Heard     int64         `json:"heard"`
	Carriers  []carrierSnap `json:"carriers"`
	TotalSent uint64        `json:"total_sent"`
	TotalRecv uint64        `json:"total_recv"`
}

type signIn struct {
	State string `json:"state"` // "login": the service wants a sign-in; "down": not connected
	Since int64  `json:"since"`
	Doc   string `json:"doc"`
}

type carrierTotal struct {
	Sent uint64 `json:"sent"`
	Recv uint64 `json:"recv"`
}

// totals adds the core's runs up: Base is what finished runs (and carriers
// replaced mid-run) counted, Last what the current run, started at Run, has.
type totals struct {
	Run  int64                   `json:"run"`
	Base map[string]carrierTotal `json:"base"`
	Last map[string]carrierTotal `json:"last"`
}

func (t *totals) add(name string, v carrierTotal) {
	b := t.Base[name]
	b.Sent += v.Sent
	b.Recv += v.Recv
	t.Base[name] = b
}

// observe takes a snapshot in; it reports whether totals.json needs rewriting.
func (t *totals) observe(s *chanStats) (changed bool) {
	if t.Base == nil {
		t.Base = map[string]carrierTotal{}
	}
	if t.Last == nil {
		t.Last = map[string]carrierTotal{}
	}
	if s.Started != t.Run { // a new run of the core: bank the last one
		for name, v := range t.Last {
			t.add(name, v)
		}
		t.Last = map[string]carrierTotal{}
		t.Run = s.Started
		changed = true
	}
	for _, c := range s.Carriers {
		cur, last := carrierTotal{Sent: c.Sent, Recv: c.Recv}, t.Last[c.Name]
		if cur == last {
			continue
		}
		if cur.Sent < last.Sent || cur.Recv < last.Recv { // the carrier was replaced mid-run
			t.add(c.Name, last)
		}
		t.Last[c.Name] = cur
		changed = true
	}
	return changed
}

func (t *totals) of(name string) carrierTotal {
	b, l := t.Base[name], t.Last[name]
	return carrierTotal{Sent: b.Sent + l.Sent, Recv: b.Recv + l.Recv}
}

// sum is the channel's traffic over every carrier ever counted (one that is
// gone from the current run still counts).
func (t *totals) sum() carrierTotal {
	var s carrierTotal
	for _, m := range []map[string]carrierTotal{t.Base, t.Last} {
		for _, v := range m {
			s.Sent += v.Sent
			s.Recv += v.Recv
		}
	}
	return s
}

func readJSONFile(path string, v interface{}) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, v)
}

// readSignIns parses signin-state: "<state> <since unix> <doc>" a line.
func readSignIns(path string) []signIn {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var out []signIn
	for _, line := range strings.Split(string(raw), "\n") {
		f := strings.Fields(line)
		if len(f) != 3 {
			continue
		}
		since, err := strconv.ParseInt(f[1], 10, 64)
		if err != nil {
			continue
		}
		out = append(out, signIn{State: f[0], Since: since, Doc: f[2]})
	}
	return out
}

// procUsage reads a process's CPU ticks (utime+stime) and resident memory from
// /proc; ok is false where there is none (not Linux, the process is gone).
func procUsage(pid int) (ticks, rss uint64, ok bool) {
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, 0, false
	}
	s := string(stat)
	i := strings.LastIndexByte(s, ')') // comm may hold spaces; fields resume after it
	if i < 0 {
		return 0, 0, false
	}
	f := strings.Fields(s[i+1:]) // f[0] is field 3 (state): utime = field 14, stime = 15
	if len(f) < 13 {
		return 0, 0, false
	}
	ut, _ := strconv.ParseUint(f[11], 10, 64)
	st, _ := strconv.ParseUint(f[12], 10, 64)
	if m, err := os.ReadFile(fmt.Sprintf("/proc/%d/statm", pid)); err == nil {
		if g := strings.Fields(string(m)); len(g) >= 2 {
			pages, _ := strconv.ParseUint(g[1], 10, 64)
			rss = pages * uint64(os.Getpagesize())
		}
	}
	return ut + st, rss, true
}

// statsLoop refreshes every channel's statistics until the process ends.
func (a *agent) statsLoop() {
	for {
		a.collect(time.Now())
		time.Sleep(a.statsEvery)
	}
}

func (a *agent) collect(now time.Time) {
	a.mu.Lock()
	chans := make([]*channel, 0, len(a.chans))
	for _, c := range a.chans {
		chans = append(chans, c)
	}
	a.mu.Unlock()
	for _, c := range chans {
		c.collect(now)
	}
}

func (c *channel) collect(now time.Time) {
	var snap chanStats
	have := readJSONFile(filepath.Join(c.dir, statsFile), &snap) == nil
	signins := readSignIns(filepath.Join(c.dir, signinFile))
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tot == nil {
		c.tot = &totals{}
		_ = readJSONFile(filepath.Join(c.dir, totalsFile), c.tot)
	}
	if have {
		if c.tot.observe(&snap) {
			if raw, err := json.Marshal(c.tot); err == nil {
				_ = writeAtomic(filepath.Join(c.dir, totalsFile), raw)
			}
		}
		for i := range snap.Carriers {
			t := c.tot.of(snap.Carriers[i].Name)
			snap.Carriers[i].TotalSent, snap.Carriers[i].TotalRecv = t.Sent, t.Recv
		}
		all := c.tot.sum()
		snap.TotalSent, snap.TotalRecv = all.Sent, all.Recv
		c.snap = &snap
	}
	c.signin = signins
	if c.pid == 0 {
		c.cpu, c.rss, c.cpuAt = 0, 0, time.Time{}
		return
	}
	if ticks, rss, ok := procUsage(c.pid); ok {
		if !c.cpuAt.IsZero() && ticks >= c.cpuTicks {
			if dt := now.Sub(c.cpuAt).Seconds(); dt > 0 {
				c.cpu = math.Round(float64(ticks-c.cpuTicks)/clkTck/dt*1000) / 10 // % of one core, 0.1
			}
		}
		c.cpuTicks, c.cpuAt, c.rss = ticks, now, rss
	}
}

// handleLog gives the channel's core output: its last tailLines lines.
func (a *agent) handleLog(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	a.mu.Lock()
	c := a.chans[name]
	a.mu.Unlock()
	if c == nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"ok": false, "error": errNoChannel.Error()})
		return
	}
	c.mu.Lock()
	lines := append([]string{}, c.tail...)
	c.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "channel": name, "lines": lines})
}
