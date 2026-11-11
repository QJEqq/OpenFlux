package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// scriptCore writes a core for the push tests: it answers --features like
// the real one and, run on a channel, prints "core <tag> up" and stays up —
// or, broken, refuses its config at once.
func scriptCore(t *testing.T, tag string, broken bool) (path, sum string) {
	t.Helper()
	body := "#!/bin/sh\nif [ \"$1\" = \"--features\" ]; then echo cookie-inbox; exit 0; fi\n"
	if broken {
		body += "echo \"core " + tag + ": config refused\"\nexit 3\n"
	} else {
		body += "echo \"core " + tag + " up\"\nexec sleep 3600\n"
	}
	path = filepath.Join(t.TempDir(), "core-"+tag)
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	sum, err := fileSHA256(path)
	if err != nil {
		t.Fatal(err)
	}
	return path, sum
}

func push(t *testing.T, a *agent, path, sum string) (int, error) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	return a.pushCore(f, sum)
}

// waitTail waits until the channel's core has printed want.
func waitTail(t *testing.T, a *agent, name, want string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		a.mu.Lock()
		c := a.chans[name]
		a.mu.Unlock()
		if c != nil {
			c.mu.Lock()
			tail := strings.Join(c.tail, "\n")
			c.mu.Unlock()
			if strings.Contains(tail, want) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: the core never printed %q", name, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// coreFiles is what dir/.core holds.
func coreFiles(a *agent) string {
	entries, _ := os.ReadDir(filepath.Join(a.dir, coreDir))
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return strings.Join(names, " ")
}

func TestPushCoreMovesChannelsOntoIt(t *testing.T) {
	dir := t.TempDir()
	ipt := &fakeIPT{rules: map[string]bool{}}
	a := newTestAgent(t, "run", dir, ipt)
	s := testSpec()
	if _, err := a.apply(s); err != nil {
		t.Fatal(err)
	}
	waitTail(t, a, s.Channel, "fake core up")

	path, sum := scriptCore(t, "B", false)
	if n, err := push(t, a, path, sum); err != nil || n != 1 {
		t.Fatalf("push: %d channels restarted, %v", n, err)
	}
	waitTail(t, a, s.Channel, "core B up")
	if _, got := a.coreNow(); got != sum || a.coreFrom() != "pushed" {
		t.Fatalf("core %.12s from %s, want the pushed %.12s", got, a.coreFrom(), sum)
	}
	if !ipt.has() {
		t.Fatal("the l3 RST rule went away on the restart")
	}
	if got := coreFiles(a); got != "current openflux-"+sum {
		t.Fatalf(".core holds %q", got)
	}

	// The next push replaces it and drops the one before.
	path, sum = scriptCore(t, "C", false)
	if _, err := push(t, a, path, sum); err != nil {
		t.Fatal(err)
	}
	waitTail(t, a, s.Channel, "core C up")
	if got := coreFiles(a); got != "current openflux-"+sum {
		t.Fatalf(".core holds %q", got)
	}

	// A container restart brings the channels back on the pushed core.
	a.stopAll()
	b := newTestAgent(t, "run", dir, ipt)
	if _, got := b.coreNow(); got != sum || b.coreFrom() != "pushed" {
		t.Fatalf("restarted on %.12s (%s), want the pushed %.12s", got, b.coreFrom(), sum)
	}
	b.restore()
	waitTail(t, b, s.Channel, "core C up")
}

func TestPushCoreRefusesWhatWouldNotRun(t *testing.T) {
	ipt := &fakeIPT{rules: map[string]bool{}}
	a := newTestAgent(t, "run", t.TempDir(), ipt)
	s := testSpec()
	if _, err := a.apply(s); err != nil {
		t.Fatal(err)
	}
	_, image := a.coreNow()

	good, _ := scriptCore(t, "D", false)
	broken, brokenSum := scriptCore(t, "E", true)
	junk := filepath.Join(t.TempDir(), "junk")
	if err := os.WriteFile(junk, []byte("not a core at all\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	junkSum, _ := fileSHA256(junk)

	for name, tc := range map[string]struct{ path, sum, err string }{
		"a cut download":     {good, strings.Repeat("0", 64), "SHA ядра не совпал"},
		"not a core":         {junk, junkSum, "не запускается на ноде"},
		"channels die on it": {broken, brokenSum, "вернул прежнее"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := push(t, a, tc.path, tc.sum); err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Fatalf("err %v, want %q", err, tc.err)
			}
			if _, got := a.coreNow(); got != image || a.coreFrom() != "image" {
				t.Fatalf("the core moved to %.12s", got)
			}
			waitState(t, a, s.Channel, "running")
			if got := coreFiles(a); got != "" {
				t.Fatalf(".core keeps %q", got)
			}
		})
	}
	waitTail(t, a, s.Channel, "fake core up") // the channel is back on the image's core
}

func TestCoreOverAPI(t *testing.T) {
	ipt := &fakeIPT{rules: map[string]bool{}}
	a := newTestAgent(t, "run", t.TempDir(), ipt)
	srv := httptest.NewServer(a.routes("tok"))
	defer srv.Close()
	call := func(method, path, sha string, body io.Reader) (int, map[string]interface{}) {
		t.Helper()
		req, _ := http.NewRequest(method, srv.URL+path, body)
		req.Header.Set("Authorization", "Bearer tok")
		if sha != "" {
			req.Header.Set("X-Core-SHA256", sha)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]interface{}
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}
	core, sum := scriptCore(t, "F", false)
	open := func() io.Reader {
		f, err := os.Open(core)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { f.Close() })
		return f
	}

	if code, out := call("PUT", "/core", "", open()); code != 400 {
		t.Fatalf("without X-Core-SHA256: %d %v", code, out)
	}
	if code, out := call("PUT", "/core", sum, open()); code != 200 || out["core"] != sum {
		t.Fatalf("push: %d %v", code, out)
	}
	code, health := call("GET", "/health", "", nil)
	if code != 200 || health["core"] != sum || health["core_from"] != "pushed" || health["agent"] != agentVersion {
		t.Fatalf("health: %d %v", code, health)
	}
	// The core it already runs: nothing restarts.
	if code, out := call("PUT", "/core", sum, open()); code != 200 || out["restarted"] != float64(0) || out["was"] != nil {
		t.Fatalf("the same core again: %d %v", code, out)
	}
}
