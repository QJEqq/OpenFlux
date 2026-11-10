package main

import (
	"bufio"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

const agentVersion = "0.1.0"

// agent keeps the node's channels: one supervised `openflux --config
// node.conf` per channel, each in its own directory under dir, so a restarted
// container brings them all back (restore).
type agent struct {
	mu    sync.Mutex
	dir   string
	bin   string
	lo    int
	hi    int
	grace time.Duration // how long a new channel's core must stay up to count as started
	core  string        // sha256 of the core binary (what /health reports)
	chans map[string]*channel

	// iptables and the node's outbound address; tests replace them.
	ipt     func(args ...string) error
	localIP func() (string, error)
}

type channel struct {
	name string
	dir  string
	port int
	mode string
	stop chan struct{}
	done chan struct{}

	mu       sync.Mutex
	state    string // running, restarting, stopped
	starts   int
	lastExit string
	tail     []string // the core's last output lines
}

type channelMeta struct {
	Port int    `json:"port"`
	Mode string `json:"mode"`
}

func newAgent(dir, bin string, lo, hi int) (*agent, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	sum := "?"
	if f, err := os.Open(bin); err == nil {
		h := sha256.New()
		_, _ = io.Copy(h, f)
		f.Close()
		sum = hex.EncodeToString(h.Sum(nil))
	}
	return &agent{
		dir: dir, bin: bin, lo: lo, hi: hi, grace: 2 * time.Second, core: sum,
		chans: map[string]*channel{}, ipt: runIptables, localIP: outboundIP,
	}, nil
}

func runIptables(args ...string) error { return exec.Command("iptables", args...).Run() }

// outboundIP is the address the node's traffic leaves from (no packet is sent).
func outboundIP() (string, error) {
	conn, err := net.Dial("udp4", "1.1.1.1:53")
	if err != nil {
		return "", err
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String(), nil
}

// restore starts every channel kept under dir (after a container restart).
func (a *agent) restore() {
	a.mu.Lock()
	defer a.mu.Unlock()
	entries, _ := os.ReadDir(a.dir)
	for _, e := range entries {
		if !e.IsDir() || !channelRe.MatchString(e.Name()) {
			continue
		}
		dir := filepath.Join(a.dir, e.Name())
		var m channelMeta
		raw, err := os.ReadFile(filepath.Join(dir, "meta.json"))
		if err != nil || json.Unmarshal(raw, &m) != nil {
			log.Printf("[ofnode] %s: нет meta.json, пропускаю", e.Name())
			continue
		}
		a.chans[e.Name()] = newChannel(e.Name(), dir, m.Port, m.Mode)
	}
	if err := a.syncL3(); err != nil {
		log.Printf("[ofnode] l3: %v", err)
	}
	for _, c := range a.chans {
		go a.supervise(c)
	}
}

func newChannel(name, dir string, port int, mode string) *channel {
	return &channel{name: name, dir: dir, port: port, mode: mode, stop: make(chan struct{}), done: make(chan struct{})}
}

// apply sets a channel up (or replaces it): writes its node.conf, key and
// cookies, starts its core, and waits until the core stays up.
func (a *agent) apply(s channelSpec) (int, error) {
	if err := s.validate(); err != nil {
		return 0, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if old := a.chans[s.Channel]; old != nil {
		a.stopLocked(old)
	}
	taken := map[int]bool{}
	for _, c := range a.chans {
		taken[c.port] = true
	}
	port := 0 // no direct: the channel listens on nothing
	if !s.NoDirect {
		var err error
		if port, err = pickPort(a.lo, a.hi, taken); err != nil {
			return 0, err
		}
	}
	dir := filepath.Join(a.dir, s.Channel)
	if err := writeChannel(dir, &s, port); err != nil {
		return 0, err
	}
	c := newChannel(s.Channel, dir, port, s.Mode)
	a.chans[s.Channel] = c
	// The RST rule before an l3 core starts, as node-install's ExecStartPre; an
	// l3 exit without it has its clients' connections reset by the kernel, so
	// no rule, no l3 channel (node-install's l3_ready refuses the same way).
	if err := a.syncL3(); err != nil && s.Mode == "l3" {
		delete(a.chans, s.Channel)
		os.RemoveAll(dir)
		return 0, fmt.Errorf("l3: %w (нужны iptables и cap_add NET_ADMIN; или режим l4)", err)
	}
	go a.supervise(c)
	if err := a.waitUp(c); err != nil {
		a.stopLocked(c)
		os.RemoveAll(dir)
		_ = a.syncL3()
		return 0, err
	}
	return port, nil
}

// writeChannel writes the channel's files into a fresh directory and swaps it
// in, so a half-written channel is never left behind.
func writeChannel(dir string, s *channelSpec, port int) error {
	tmp := dir + ".new"
	os.RemoveAll(tmp)
	if err := os.MkdirAll(tmp, 0o700); err != nil {
		return err
	}
	files := map[string][]byte{
		"encryption-key": []byte(s.Key + "\n"),
		"node.conf":      []byte(nodeConf(s, dir, port)),
	}
	meta, _ := json.Marshal(channelMeta{Port: port, Mode: s.Mode})
	files["meta.json"] = meta
	if s.Cookies != "" {
		raw, _ := base64.StdEncoding.DecodeString(s.Cookies) // validated already
		files["cookies.json"] = raw
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(tmp, name), data, 0o600); err != nil {
			os.RemoveAll(tmp)
			return err
		}
	}
	os.RemoveAll(dir)
	return os.Rename(tmp, dir)
}

// remove stops a channel's core and deletes the channel.
func (a *agent) remove(name string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	c := a.chans[name]
	if c == nil {
		return errNoChannel
	}
	a.stopLocked(c)
	os.RemoveAll(c.dir)
	_ = a.syncL3()
	return nil
}

var errNoChannel = errors.New("такого канала на ноде нет")

// stopAll stops every core but keeps the channels on disk (container stop).
func (a *agent) stopAll() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, c := range a.chans {
		a.stopLocked(c)
	}
	_ = a.syncL3()
}

// stopLocked stops a channel's supervisor and forgets the channel. Caller holds a.mu.
func (a *agent) stopLocked(c *channel) {
	close(c.stop)
	<-c.done
	delete(a.chans, c.name)
}

// supervise runs the channel's core and restarts it when it dies, backing off
// from 1 s to 30 s (back to 1 s after a minute of uptime).
func (a *agent) supervise(c *channel) {
	defer close(c.done)
	backoff := time.Second
	for {
		begun := time.Now()
		err := a.runOnce(c)
		if err == nil { // stopped on request
			c.set("stopped", "")
			return
		}
		c.set("restarting", err.Error())
		if time.Since(begun) > time.Minute {
			backoff = time.Second
		}
		select {
		case <-c.stop:
			c.set("stopped", "")
			return
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, 30*time.Second)
	}
}

// runOnce runs the core until it exits (an error) or the channel is stopped (nil).
func (a *agent) runOnce(c *channel) error {
	r, w, err := os.Pipe()
	if err != nil {
		return err
	}
	cmd := exec.Command(a.bin, "--config", filepath.Join(c.dir, "node.conf"))
	cmd.Dir = c.dir
	cmd.Stdout, cmd.Stderr = w, w
	c.mu.Lock()
	c.starts++
	c.mu.Unlock()
	if err := cmd.Start(); err != nil {
		r.Close()
		w.Close()
		return err
	}
	w.Close()
	pumped := make(chan struct{})
	go func() { a.pump(c, r); close(pumped) }()
	c.set("running", "")
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	select {
	case <-c.stop:
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-exited:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-exited
		}
		<-pumped
		return nil
	case err := <-exited:
		<-pumped
		if err == nil {
			err = errors.New("ядро вышло")
		}
		return err
	}
}

// pump forwards the core's output to the agent's log (docker logs) and keeps
// its last lines for error messages.
func (a *agent) pump(c *channel, r io.ReadCloser) {
	defer r.Close()
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		c.mu.Lock()
		c.tail = append(c.tail, line)
		if len(c.tail) > 20 {
			c.tail = c.tail[len(c.tail)-20:]
		}
		c.mu.Unlock()
		log.Printf("[%s] %s", c.name, line)
	}
}

func (c *channel) set(state, why string) {
	c.mu.Lock()
	c.state = state
	if why != "" {
		c.lastExit = why
	}
	c.mu.Unlock()
}

type channelStatus struct {
	Channel  string `json:"channel"`
	Port     int    `json:"port"`
	Mode     string `json:"mode"`
	State    string `json:"state"`
	Restarts int    `json:"restarts"`
	LastExit string `json:"last_exit,omitempty"`
}

func (c *channel) status() channelStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	return channelStatus{Channel: c.name, Port: c.port, Mode: c.mode, State: c.state, Restarts: max(c.starts-1, 0), LastExit: c.lastExit}
}

// waitUp waits a.grace for the new core to stay up; a core that exits at once
// (a config it refuses) fails the apply with its last output lines.
func (a *agent) waitUp(c *channel) error {
	deadline := time.Now().Add(a.grace)
	for {
		c.mu.Lock()
		state, starts, last := c.state, c.starts, c.lastExit
		tail := append([]string(nil), c.tail...)
		c.mu.Unlock()
		if state == "restarting" || starts > 1 {
			if len(tail) > 3 {
				tail = tail[len(tail)-3:]
			}
			return fmt.Errorf("ядро канала не поднялось (%s): %s", last, strings.Join(tail, " | "))
		}
		if time.Now().After(deadline) {
			if state != "running" {
				return fmt.Errorf("ядро канала не запустилось за %v", a.grace)
			}
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// syncL3 keeps the RST-drop rule for the node's address while any l3 channel
// is set up: an l3 exit answers its clients' packets itself, so the kernel
// must not reset those connections (node-install.sh's l3-rst.sh). It returns
// why the rule an l3 channel needs could not be put in place. Caller holds a.mu.
func (a *agent) syncL3() error {
	need := false
	for _, c := range a.chans {
		if c.mode == "l3" {
			need = true
		}
	}
	ip, err := a.localIP()
	if err != nil {
		if need {
			return fmt.Errorf("адрес ноды не найден: %w", err)
		}
		return nil
	}
	rule := []string{"OUTPUT", "-p", "tcp", "--tcp-flags", "RST", "RST", "-s", ip, "-j", "DROP"}
	has := a.ipt(append([]string{"-C"}, rule...)...) == nil
	switch {
	case need && !has:
		if err := a.ipt(append([]string{"-A"}, rule...)...); err != nil {
			return fmt.Errorf("не удалось добавить правило RST: %w", err)
		}
	case !need && has:
		if err := a.ipt(append([]string{"-D"}, rule...)...); err != nil {
			log.Printf("[ofnode] l3: не удалось убрать правило RST: %v", err)
		}
	}
	return nil
}

// ---- HTTP API --------------------------------------------------------------

func (a *agent) routes(token string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", a.handleHealth)
	mux.HandleFunc("GET /channels", a.handleList)
	mux.HandleFunc("POST /channels", a.handleApply)
	mux.HandleFunc("DELETE /channels/{name}", a.handleRemove)
	want := []byte("Bearer " + token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1 {
			writeJSON(w, http.StatusUnauthorized, map[string]interface{}{"ok": false, "error": "unauthorized"})
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (a *agent) handleHealth(w http.ResponseWriter, _ *http.Request) {
	a.mu.Lock()
	n := len(a.chans)
	a.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"ok": true, "agent": agentVersion, "core": a.core, "arch": runtime.GOARCH, "channels": n,
		"port_range": fmt.Sprintf("%d-%d", a.lo, a.hi),
	})
}

func (a *agent) handleList(w http.ResponseWriter, _ *http.Request) {
	a.mu.Lock()
	out := make([]channelStatus, 0, len(a.chans))
	for _, c := range a.chans {
		out = append(out, c.status())
	}
	a.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Channel < out[j].Channel })
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "channels": out})
}

func (a *agent) handleApply(w http.ResponseWriter, r *http.Request) {
	var s channelSpec
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 256<<10)).Decode(&s); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "тело не JSON канала: " + err.Error()})
		return
	}
	port, err := a.apply(s)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": err.Error()})
		return
	}
	direct := fmt.Sprintf("+ direct :%d", port)
	if s.NoDirect {
		direct = "без direct, портов не открыто"
	}
	log.Printf("[ofnode] + %s: режим %s, %d носителей %s", s.Channel, s.Mode, len(s.Transports), direct)
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "channel": s.Channel, "port": port})
}

func (a *agent) handleRemove(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := a.remove(name); err != nil {
		code := http.StatusInternalServerError
		if errors.Is(err, errNoChannel) {
			code = http.StatusNotFound
		}
		writeJSON(w, code, map[string]interface{}{"ok": false, "error": err.Error()})
		return
	}
	log.Printf("[ofnode] - %s", name)
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "channel": name})
}
