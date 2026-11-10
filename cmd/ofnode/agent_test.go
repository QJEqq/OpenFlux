package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// The test binary doubles as the core: OFNODE_FAKE_CORE=run stays up until
// SIGTERM, =fail refuses its config at once.
func TestMain(m *testing.M) {
	switch os.Getenv("OFNODE_FAKE_CORE") {
	case "run":
		fmt.Println("fake core up:", strings.Join(os.Args[1:], " "))
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGTERM, os.Interrupt)
		<-sig
		os.Exit(0)
	case "fail":
		fmt.Println("boom: config refused")
		os.Exit(3)
	}
	os.Exit(m.Run())
}

const (
	testBoard = "https://boards.yandex.ru/whiteboard/?hash=0123456789abcdef0123456789abcdef"
	testVolga = "https://docs.yandex.ru/edit/d/AbCdEfGhIjKlMnOpQrStUv?source=docs"
)

func testSpec() channelSpec {
	store, _ := json.Marshal(map[string]map[string]string{testVolga: {"Session_id": "s"}})
	return channelSpec{
		Channel: "u3ca4de4368d665e7", Key: strings.Repeat("ab", 32), Mode: "l3",
		Transports: []transportSpec{{Type: "boards", URL: testBoard}, {Type: "vyandex", URL: testVolga}},
		Cookies:    base64.StdEncoding.EncodeToString(store),
		Priorities: map[string]int{"direct": 100, "boards": 70, "vyandex": 50},
	}
}

// fakeIPT is iptables with the rules kept in a map.
type fakeIPT struct {
	mu    sync.Mutex
	rules map[string]bool
}

func (f *fakeIPT) run(args ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := strings.Join(args[1:], " ")
	switch args[0] {
	case "-C":
		if !f.rules[key] {
			return errors.New("no rule")
		}
	case "-A":
		f.rules[key] = true
	case "-D":
		delete(f.rules, key)
	}
	return nil
}

func (f *fakeIPT) has() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rules["OUTPUT -p tcp --tcp-flags RST RST -s 10.0.0.1 -j DROP"]
}

func newTestAgent(t *testing.T, core, dir string, ipt *fakeIPT) *agent {
	t.Helper()
	t.Setenv("OFNODE_FAKE_CORE", core)
	a, err := newAgent(dir, os.Args[0], 30000, 30999)
	if err != nil {
		t.Fatal(err)
	}
	a.grace = 400 * time.Millisecond
	a.ipt = ipt.run
	a.localIP = func() (string, error) { return "10.0.0.1", nil }
	t.Cleanup(a.stopAll)
	return a
}

func TestNodeConfLikeNodeInstall(t *testing.T) {
	s := testSpec()
	if err := s.validate(); err != nil {
		t.Fatal(err)
	}
	want := `# OpenFlux node channel u3ca4de4368d665e7, written by ofnode
Role = exit
Mode = l3
EncryptionKeyFile = /d/encryption-key
CookieStore = /d/cookies.json
URL = ` + testVolga + `

[Transport vyandex]
Type = vyandex
Priority = 50
URL = ` + testVolga + `

[Transport boards]
Type = boards
Priority = 70
URL = ` + testBoard + `

[Transport direct]
Type = direct
Priority = 100
Listen = 0.0.0.0:31000
`
	if got := nodeConf(&s, "/d", 31000); got != want {
		t.Fatalf("node.conf:\n%s\nwant:\n%s", got, want)
	}
	// No priorities: node-install.sh's defaults.
	s.Priorities = nil
	if got := nodeConf(&s, "/d", 31000); !strings.Contains(got, "Type = vyandex\nPriority = 100") || !strings.Contains(got, "Type = direct\nPriority = 50") {
		t.Fatalf("default priorities:\n%s", got)
	}
}

func TestValidateRefuses(t *testing.T) {
	for name, edit := range map[string]func(*channelSpec){
		"channel":       func(s *channelSpec) { s.Channel = "U-BAD" },
		"key":           func(s *channelSpec) { s.Key = "abc" },
		"mode":          func(s *channelSpec) { s.Mode = "l5" },
		"direct listed": func(s *channelSpec) { s.Transports = append(s.Transports, transportSpec{Type: "direct", URL: "x"}) },
		"unknown":       func(s *channelSpec) { s.Transports = append(s.Transports, transportSpec{Type: "oneme", URL: "x"}) },
		"twice": func(s *channelSpec) {
			s.Transports = append(s.Transports, transportSpec{Type: "boards", URL: testBoard})
		},
		"board url":     func(s *channelSpec) { s.Transports[0].URL = "https://boards.yandex.ru/whiteboard/?hash=1" },
		"volga query":   func(s *channelSpec) { s.Transports[1].URL = strings.Replace(testVolga, "source=docs", "sk=1", 1) },
		"no doc login":  func(s *channelSpec) { s.Transports = s.Transports[:1] },
		"cookies":       func(s *channelSpec) { s.Cookies = "not base64!" },
		"priority":      func(s *channelSpec) { s.Priorities = map[string]int{"direct": 1000} },
		"priority type": func(s *channelSpec) { s.Priorities = map[string]int{"script": 1} },
	} {
		s := testSpec()
		edit(&s)
		if err := s.validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func waitState(t *testing.T, a *agent, name, state string) channelStatus {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		a.mu.Lock()
		c := a.chans[name]
		a.mu.Unlock()
		if c != nil {
			if st := c.status(); st.State == state {
				return st
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never reached %q", name, state)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestApplyListRemove(t *testing.T) {
	ipt := &fakeIPT{rules: map[string]bool{}}
	a := newTestAgent(t, "run", t.TempDir(), ipt)
	s := testSpec()
	port, err := a.apply(s)
	if err != nil {
		t.Fatal(err)
	}
	if port < 30000 || port > 30999 {
		t.Fatalf("port %d outside the range", port)
	}
	st := waitState(t, a, s.Channel, "running")
	if st.Port != port || st.Mode != "l3" {
		t.Fatalf("status %+v", st)
	}
	dir := filepath.Join(a.dir, s.Channel)
	conf, _ := os.ReadFile(filepath.Join(dir, "node.conf"))
	if !strings.Contains(string(conf), fmt.Sprintf("Listen = 0.0.0.0:%d", port)) {
		t.Fatalf("node.conf:\n%s", conf)
	}
	if fi, err := os.Stat(filepath.Join(dir, "encryption-key")); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("encryption-key: %v %v", fi, err)
	}
	if raw, _ := os.ReadFile(filepath.Join(dir, "cookies.json")); !bytes.Contains(raw, []byte("Session_id")) {
		t.Fatalf("cookies.json: %s", raw)
	}
	if !ipt.has() {
		t.Fatal("no RST rule for an l3 channel")
	}
	// The core was started on this channel's node.conf.
	a.mu.Lock()
	tail := strings.Join(a.chans[s.Channel].tail, "\n")
	a.mu.Unlock()
	if !strings.Contains(tail, "--config "+filepath.Join(dir, "node.conf")) {
		t.Fatalf("core output: %q", tail)
	}
	if err := a.remove(s.Channel); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) || ipt.has() {
		t.Fatalf("after remove: dir %v, rule %v", err, ipt.has())
	}
	if err := a.remove(s.Channel); !errors.Is(err, errNoChannel) {
		t.Fatalf("second remove: %v", err)
	}
}

func TestApplyReportsACoreThatDies(t *testing.T) {
	ipt := &fakeIPT{rules: map[string]bool{}}
	a := newTestAgent(t, "fail", t.TempDir(), ipt)
	s := testSpec()
	if _, err := a.apply(s); err == nil || !strings.Contains(err.Error(), "boom: config refused") {
		t.Fatalf("err %v", err)
	}
	if _, err := os.Stat(filepath.Join(a.dir, s.Channel)); !os.IsNotExist(err) || len(a.chans) != 0 || ipt.has() {
		t.Fatalf("left behind: dir %v, chans %d, rule %v", err, len(a.chans), ipt.has())
	}
}

func TestRestoreBringsChannelsBack(t *testing.T) {
	dir := t.TempDir()
	ipt := &fakeIPT{rules: map[string]bool{}}
	a := newTestAgent(t, "run", dir, ipt)
	s := testSpec()
	port, err := a.apply(s)
	if err != nil {
		t.Fatal(err)
	}
	a.stopAll() // the container stops: cores down, channels kept on disk
	if ipt.has() {
		t.Fatal("RST rule kept after every core stopped")
	}
	b := newTestAgent(t, "run", dir, ipt)
	b.restore()
	if st := waitState(t, b, s.Channel, "running"); st.Port != port {
		t.Fatalf("restored %+v, want port %d", st, port)
	}
	if !ipt.has() {
		t.Fatal("RST rule not back after restore")
	}
}

func TestAPIOverPinnedTLS(t *testing.T) {
	sec, err := newSecret()
	if err != nil {
		t.Fatal(err)
	}
	parsed, cert, err := parseSecret(sec.encode())
	if err != nil || parsed.Token != sec.Token {
		t.Fatalf("parseSecret: %v", err)
	}
	a := newTestAgent(t, "run", t.TempDir(), &fakeIPT{rules: map[string]bool{}})
	srv := httptest.NewUnstartedServer(a.routes(sec.Token))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	srv.StartTLS()
	defer srv.Close()

	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM([]byte(sec.Cert))
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: certName}}}
	call := func(method, path, token string, body interface{}) (int, map[string]interface{}) {
		var rd *bytes.Reader
		if body != nil {
			raw, _ := json.Marshal(body)
			rd = bytes.NewReader(raw)
		} else {
			rd = bytes.NewReader(nil)
		}
		req, _ := http.NewRequest(method, srv.URL+path, rd)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]interface{}
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}
	if code, _ := call("GET", "/health", "", nil); code != http.StatusUnauthorized {
		t.Fatalf("no token: %d", code)
	}
	if code, _ := call("GET", "/health", "wrong", nil); code != http.StatusUnauthorized {
		t.Fatalf("wrong token: %d", code)
	}
	if code, out := call("GET", "/health", sec.Token, nil); code != 200 || out["ok"] != true || out["agent"] != agentVersion {
		t.Fatalf("health: %d %v", code, out)
	}
	s := testSpec()
	code, out := call("POST", "/channels", sec.Token, s)
	if code != 200 || out["ok"] != true || out["port"] == nil {
		t.Fatalf("apply: %d %v", code, out)
	}
	if code, out := call("GET", "/channels", sec.Token, nil); code != 200 || !strings.Contains(fmt.Sprint(out["channels"]), s.Channel) {
		t.Fatalf("list: %d %v", code, out)
	}
	bad := testSpec()
	bad.Key = "x"
	if code, out := call("POST", "/channels", sec.Token, bad); code != 400 || out["ok"] != false {
		t.Fatalf("bad apply: %d %v", code, out)
	}
	if code, _ := call("DELETE", "/channels/"+s.Channel, sec.Token, nil); code != 200 {
		t.Fatalf("remove: %d", code)
	}
	if code, _ := call("DELETE", "/channels/"+s.Channel, sec.Token, nil); code != 404 {
		t.Fatalf("second remove: %d", code)
	}

	// A client that pinned another node's certificate does not get in.
	other, _ := newSecret()
	pool2 := x509.NewCertPool()
	pool2.AppendCertsFromPEM([]byte(other.Cert))
	c2 := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool2, ServerName: certName}}}
	if _, err := c2.Get(srv.URL + "/health"); err == nil {
		t.Fatal("TLS accepted a certificate the client did not pin")
	}
	if _, _, err := parseSecret("not a secret"); err == nil {
		t.Fatal("garbage SECRET_KEY accepted")
	}
}

// An l3 exit without the RST rule has its clients' connections reset by the
// kernel: when iptables refuses (no NET_ADMIN), an l3 channel is refused too,
// cleanly, and l4 still works.
func TestL3NeedsTheRSTRule(t *testing.T) {
	ipt := &fakeIPT{rules: map[string]bool{}}
	a := newTestAgent(t, "run", t.TempDir(), ipt)
	a.ipt = func(args ...string) error {
		if args[0] == "-A" {
			return errors.New("iptables: Permission denied")
		}
		return ipt.run(args...)
	}
	s := testSpec()
	if _, err := a.apply(s); err == nil || !strings.Contains(err.Error(), "l3:") || !strings.Contains(err.Error(), "NET_ADMIN") {
		t.Fatalf("l3 without the rule: %v", err)
	}
	if _, err := os.Stat(filepath.Join(a.dir, s.Channel)); !os.IsNotExist(err) || len(a.chans) != 0 {
		t.Fatalf("left behind: dir %v, chans %d", err, len(a.chans))
	}
	s.Mode = "l4"
	if _, err := a.apply(s); err != nil {
		t.Fatalf("l4: %v", err)
	}
}

// No direct: many open odd ports give a node away to DPI, so the channel
// listens on nothing and rides only its relays.
func TestNoDirectOpensNoPort(t *testing.T) {
	a := newTestAgent(t, "run", t.TempDir(), &fakeIPT{rules: map[string]bool{}})
	s := testSpec()
	s.NoDirect = true
	port, err := a.apply(s)
	if err != nil || port != 0 {
		t.Fatalf("apply: port %d, err %v", port, err)
	}
	conf, _ := os.ReadFile(filepath.Join(a.dir, s.Channel, "node.conf"))
	if strings.Contains(string(conf), "[Transport direct]") || strings.Contains(string(conf), "Listen =") ||
		!strings.Contains(string(conf), "[Transport boards]") {
		t.Fatalf("node.conf:\n%s", conf)
	}
	if st := waitState(t, a, s.Channel, "running"); st.Port != 0 {
		t.Fatalf("status %+v", st)
	}
	empty := testSpec()
	empty.NoDirect, empty.Transports, empty.Cookies = true, nil, ""
	if err := empty.validate(); err == nil {
		t.Fatal("a channel with no carrier at all accepted")
	}
}
