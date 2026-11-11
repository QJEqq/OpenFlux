package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func snapOf(started int64, carriers ...carrierSnap) *chanStats {
	return &chanStats{At: started + 1, Started: started, Carriers: carriers}
}

// The core's counters start from zero with each run; the totals add runs up,
// and a carrier replaced mid-run (its counters drop) is banked too.
func TestTotalsAddRunsUp(t *testing.T) {
	var tot totals
	steps := []struct {
		snap      *chanStats
		sent, rcv uint64
	}{
		{snapOf(10, carrierSnap{Name: "boards", Sent: 100, Recv: 200}), 100, 200},
		{snapOf(10, carrierSnap{Name: "boards", Sent: 150, Recv: 260}), 150, 260}, // same run grows
		{snapOf(10, carrierSnap{Name: "boards", Sent: 150, Recv: 260}), 150, 260}, // nothing new
		{snapOf(20, carrierSnap{Name: "boards", Sent: 30, Recv: 40}), 180, 300},   // a new run
		{snapOf(20, carrierSnap{Name: "boards", Sent: 5, Recv: 6}), 185, 306},     // carrier replaced
	}
	for i, s := range steps {
		changed := tot.observe(s.snap)
		if got := tot.of("boards"); got.Sent != s.sent || got.Recv != s.rcv {
			t.Fatalf("step %d: %+v, want %d/%d", i, got, s.sent, s.rcv)
		}
		if i == 2 && changed {
			t.Fatal("an unchanged snapshot rewrote totals.json")
		}
	}
}

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func statsJSON(t *testing.T, s chanStats) string {
	t.Helper()
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestCollectChannelStats(t *testing.T) {
	dir := t.TempDir()
	ipt := &fakeIPT{rules: map[string]bool{}}
	a := newTestAgent(t, "run", dir, ipt)
	s := testSpec()
	if _, err := a.apply(s); err != nil {
		t.Fatal(err)
	}
	ch := filepath.Join(dir, s.Channel)
	writeFile(t, filepath.Join(ch, statsFile), statsJSON(t, chanStats{At: 2000, Started: 1000, Mode: "session", Client: true,
		Active: "boards", Heard: 1990, Carriers: []carrierSnap{
			{Name: "boards", Priority: 70, Up: true, Heard: 1990, Sent: 5000, Recv: 700},
			{Name: "vyandex", Priority: 50, Up: false, Sent: 10, Recv: 20}}}))
	writeFile(t, filepath.Join(ch, signinFile), "login 1500 "+testVolga+"\nmalformed line\n")
	a.collect(time.Now())

	st := a.chans[s.Channel].status()
	if st.Stats == nil || st.Stats.Active != "boards" || !st.Stats.Client || st.Stats.Heard != 1990 || len(st.Stats.Carriers) != 2 {
		t.Fatalf("stats %+v", st.Stats)
	}
	if st.Stats.TotalSent != 5010 || st.Stats.TotalRecv != 720 || st.Stats.Carriers[0].TotalSent != 5000 {
		t.Fatalf("totals %+v", st.Stats)
	}
	if len(st.SignIn) != 1 || st.SignIn[0].State != "login" || st.SignIn[0].Since != 1500 || st.SignIn[0].Doc != testVolga {
		t.Fatalf("signin %+v", st.SignIn)
	}
	if st.Since == 0 {
		t.Fatal("no start time for a running core")
	}

	// The core restarted: its counters start over, the totals keep counting —
	// in totals.json, so an agent restart keeps them too.
	writeFile(t, filepath.Join(ch, statsFile), statsJSON(t, chanStats{At: 3000, Started: 2500, Carriers: []carrierSnap{
		{Name: "boards", Sent: 100, Recv: 50}}}))
	a.collect(time.Now())
	if st := a.chans[s.Channel].status(); st.Stats.TotalSent != 5110 || st.Stats.Carriers[0].TotalRecv != 750 {
		t.Fatalf("after a core restart: %+v", st.Stats)
	}
	a.stopAll()
	b := newTestAgent(t, "run", dir, ipt)
	b.restore()
	waitState(t, b, s.Channel, "running")
	b.collect(time.Now())
	if st := b.chans[s.Channel].status(); st.Stats == nil || st.Stats.TotalSent != 5110 {
		t.Fatalf("after an agent restart: %+v", st.Stats)
	}
}

func TestChannelLogOverAPI(t *testing.T) {
	ipt := &fakeIPT{rules: map[string]bool{}}
	a := newTestAgent(t, "run", t.TempDir(), ipt)
	s := testSpec()
	if _, err := a.apply(s); err != nil {
		t.Fatal(err)
	}
	waitTail(t, a, s.Channel, "fake core up")
	srv := httptest.NewServer(a.routes("tok"))
	defer srv.Close()
	get := func(path string) (int, map[string]interface{}) {
		req, _ := http.NewRequest("GET", srv.URL+path, nil)
		req.Header.Set("Authorization", "Bearer tok")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]interface{}
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}
	code, out := get("/channels/" + s.Channel + "/log")
	lines, _ := out["lines"].([]interface{})
	if code != 200 || len(lines) == 0 || !strings.Contains(lines[0].(string), "fake core up") {
		t.Fatalf("log: %d %v", code, out)
	}
	if code, _ := get("/channels/nope/log"); code != 404 {
		t.Fatalf("unknown channel: %d", code)
	}
}

func TestProcUsage(t *testing.T) {
	if runtime.GOOS != "linux" {
		if _, _, ok := procUsage(os.Getpid()); ok {
			t.Fatal("procUsage says ok without /proc")
		}
		t.Skip("/proc is Linux-only")
	}
	ticks, rss, ok := procUsage(os.Getpid())
	if !ok || rss == 0 {
		t.Fatalf("procUsage: ticks %d rss %d ok %v", ticks, rss, ok)
	}
}
