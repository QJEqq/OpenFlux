package yandex

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/p1neappleXpress/OpenFlux/transport"
)

// fakeSocketBoard is the board's socket.io server as it answers live
// (2026-11): "im" subscribe, then subscribe-slide-dashboard confirmed by the
// joiner's own participant-connected, with the snapshot (ack 431: every
// object on the board) sent after snapDelay - or never, or (an older server)
// at once and alone. modify-objects and drop-objects change the board and
// are broadcast to every socket, the sender's own included.
type fakeSocketBoard struct {
	mu      sync.Mutex
	objects map[string]json.RawMessage // hash -> object
	socks   map[*fakeSock]bool
	sockets int // sockets accepted so far

	snapshot  string // "later" (default), "never" or "first"
	snapDelay time.Duration
	refuse    bool // answer new sockets 503

	// The server's own limit, per socket, as live: an event over it is
	// answered RATE_LIMITED and dropped. 0: none.
	limitRate, limitBurst float64
	rateLimited           int

	dropEntries []json.RawMessage // every entry of every drop-objects
	dropEvents  int
	modifies    int
}

type fakeSock struct {
	ws   *websocket.Conn
	wmu  sync.Mutex
	name string
	hash string

	limit *boardsPace // the server's budget for this socket
}

func (s *fakeSock) send(msg string) {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	_ = s.ws.WriteMessage(websocket.TextMessage, []byte(msg))
}

func newFakeSocketBoard() *fakeSocketBoard {
	return &fakeSocketBoard{objects: map[string]json.RawMessage{}, socks: map[*fakeSock]bool{}, snapshot: "later"}
}

// put lays an object on the board as if someone had sent it.
func (f *fakeSocketBoard) put(hash, style string) {
	obj := map[string]interface{}{
		"_attributes_": map[string]interface{}{"id": hash, "value": "QUJD", "style": style, "type": "textbox"},
		"mxGeometry":   []interface{}{},
		"hash":         hash,
	}
	raw, _ := json.Marshal(obj)
	f.mu.Lock()
	f.objects[hash] = raw
	f.mu.Unlock()
}

func (f *fakeSocketBoard) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.objects)
}

func (f *fakeSocketBoard) peers() []*fakeSock {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*fakeSock, 0, len(f.socks))
	for s := range f.socks {
		out = append(out, s)
	}
	return out
}

func (f *fakeSocketBoard) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	if f.refuse {
		f.mu.Unlock()
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	f.sockets++
	f.mu.Unlock()
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	ws, err := up.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	s := &fakeSock{ws: ws}
	defer func() {
		f.mu.Lock()
		delete(f.socks, s)
		f.mu.Unlock()
		ws.Close()
	}()
	s.send(`0{"sid":"eio","upgrades":[],"pingInterval":25000,"pingTimeout":30000}`)
	for {
		_, m, err := ws.ReadMessage()
		if err != nil {
			return
		}
		f.handle(s, string(m))
	}
}

func (f *fakeSocketBoard) snapshotBody(s *fakeSock, old bool) string {
	f.mu.Lock()
	objs := make([]json.RawMessage, 0, len(f.objects))
	for _, o := range f.objects {
		objs = append(objs, o)
	}
	f.mu.Unlock()
	part := map[string]interface{}{"socket": "eio", "name": s.name, "session": "", "participantHash": s.hash, "dashboard": "dash"}
	if old {
		part = map[string]interface{}{"hash": s.hash, "session": "sess", "participantHash": s.hash,
			"dashboard_link": map[string]string{"session": "sess", "dashboard": "dash"}}
	}
	body, _ := json.Marshal([]interface{}{map[string]interface{}{"participant": part, "objects": objs, "dialogs": []interface{}{}}})
	return string(body)
}

func (f *fakeSocketBoard) broadcast(action string, s *fakeSock, objects interface{}) {
	out, _ := json.Marshal([]interface{}{"dashboard", map[string]interface{}{
		"action": action,
		"data":   map[string]interface{}{"session": "", "participant": s.hash, "name": s.name, "objects": objects},
	}})
	for _, p := range f.peers() {
		p.send("42" + string(out))
	}
}

func (f *fakeSocketBoard) handle(s *fakeSock, m string) {
	switch {
	case m == "40":
		s.send(`40{"sid":"nsp"}`)
		return
	case !strings.HasPrefix(m, "42"):
		return
	}
	i := strings.IndexByte(m, '[')
	ack := m[2:i]
	var arr []json.RawMessage
	if json.Unmarshal([]byte(m[i:]), &arr) != nil || len(arr) < 2 {
		return
	}
	var event string
	_ = json.Unmarshal(arr[0], &event)
	if event == "im" {
		s.send(`42["subscribed"]`)
		s.send("43" + ack + `[{"subscribed":true}]`)
		return
	}
	var env struct {
		Action string          `json:"action"`
		Data   json.RawMessage `json:"data"`
	}
	_ = json.Unmarshal(arr[1], &env)
	var data struct {
		Objects []json.RawMessage `json:"objects"`
		Options struct {
			Participant struct {
				Name string `json:"name"`
			} `json:"participant"`
		} `json:"options"`
	}
	_ = json.Unmarshal(env.Data, &data)
	if env.Action == "modify-objects" || env.Action == "drop-objects" {
		f.mu.Lock()
		if f.limitRate > 0 && s.limit == nil {
			s.limit = newBoardsPace(f.limitRate, f.limitBurst)
		}
		over := s.limit != nil && s.limit.take(time.Now(), 0) > 0
		if over {
			f.rateLimited++
		}
		f.mu.Unlock()
		if over {
			s.send("43" + ack + `[{"error":{"code":200,"securityCode":"RATE_LIMITED"}}]`)
			return
		}
	}
	switch env.Action {
	case "subscribe-slide-dashboard":
		f.mu.Lock()
		s.name = data.Options.Participant.Name
		s.hash = fmt.Sprintf("%032x", f.sockets)
		f.socks[s] = true
		mode, delay := f.snapshot, f.snapDelay
		f.mu.Unlock()
		if mode == "first" {
			s.send("43" + ack + f.snapshotBody(s, true))
			return
		}
		s.send(`42["dashboard",{"action":"participant-rights-snapshot-supported","data":{}}]`)
		joined := fmt.Sprintf(`42["dashboard",{"action":"participant-connected","data":{"participant":{"session":"","hash":%q,"name":%q,"editor":true}}}]`, s.hash, s.name)
		for _, p := range f.peers() {
			p.send(joined)
		}
		if mode == "never" {
			return
		}
		snap := f.snapshotBody(s, false)
		go func() {
			time.Sleep(delay)
			s.send("43" + ack + snap)
		}()
	case "modify-objects":
		f.mu.Lock()
		f.modifies++
		for _, o := range data.Objects {
			var h struct {
				Hash string `json:"hash"`
			}
			_ = json.Unmarshal(o, &h)
			f.objects[h.Hash] = o
		}
		f.mu.Unlock()
		f.broadcast("server-modify-objects", s, data.Objects)
		s.send("43" + ack + `[{"result":true}]`)
	case "drop-objects":
		var gone []map[string]string
		f.mu.Lock()
		f.dropEvents++
		for _, o := range data.Objects {
			f.dropEntries = append(f.dropEntries, o)
			var h struct {
				Hash string `json:"hash"`
			}
			_ = json.Unmarshal(o, &h)
			delete(f.objects, h.Hash)
			gone = append(gone, map[string]string{"hash": h.Hash})
		}
		f.mu.Unlock()
		f.broadcast("server-drop-objects", s, gone)
		s.send("43" + ack + `[{"result":true}]`)
	default:
		s.send("43" + ack + `[{"result":true}]`)
	}
}

// startFakeBoard serves f over TLS and points the board socket's TLS at it.
func startFakeBoard(t *testing.T, f *fakeSocketBoard) string {
	t.Helper()
	srv := httptest.NewTLSServer(f)
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	prevTLS, prevReauth := boardsTLS, boardsReauthAfter
	boardsTLS = &tls.Config{RootCAs: pool}
	boardsReauthAfter = 1 << 30 // never sign in again: the sign-in is real HTTP
	t.Cleanup(func() {
		srv.CloseClientConnections()
		srv.Close()
		boardsTLS, boardsReauthAfter = prevTLS, prevReauth
	})
	return strings.TrimPrefix(srv.URL, "https://")
}

// joinFakeBoard starts a carrier on the fake board as Start would after the
// guest sign-in.
func joinFakeBoard(t *testing.T, host, name string) *BoardsTransport {
	t.Helper()
	tr := NewBoardsTransport("https://"+boardsBase+"/whiteboard/?hash="+testBoardHash, transport.DefaultConfig())
	if err := tr.BaseTransport.Start(); err != nil {
		t.Fatal(err)
	}
	info := boardsInfo{hash: testBoardHash, name: name, userHash: "user-" + name, jwt: "jwt",
		wsHost: host, dashboard: "dash", currentSlide: "dash"}
	go tr.connectLoop(info)
	t.Cleanup(func() { _ = tr.Stop() })
	return tr
}

func waitFor(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within %v", what, within)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func boardsCollect(tr *BoardsTransport) (func() [][]byte, func()) {
	var mu sync.Mutex
	var got [][]byte
	tr.Receive(func(p []byte) {
		mu.Lock()
		got = append(got, append([]byte(nil), p...))
		mu.Unlock()
	})
	return func() [][]byte {
			mu.Lock()
			defer mu.Unlock()
			return append([][]byte(nil), got...)
		}, func() {
			mu.Lock()
			got = nil
			mu.Unlock()
		}
}

// The live failure: the server confirms the join with participant-connected
// and the snapshot never comes in time. The carrier must not wait for it.
func TestBoardsJoinsWithoutSnapshot(t *testing.T) {
	f := newFakeSocketBoard()
	f.snapshot = "never"
	host := startFakeBoard(t, f)
	a := joinFakeBoard(t, host, "guest_aaaaaa")
	b := joinFakeBoard(t, host, "guest_bbbbbb")
	began := time.Now()
	waitFor(t, 3*time.Second, "both carriers up", func() bool { return a.IsConnected() && b.IsConnected() })
	if d := time.Since(began); d > 2*time.Second {
		t.Fatalf("join took %v", d)
	}
	got, _ := boardsCollect(b)
	if err := a.Send([]byte("hello through the board")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, "packet delivered", func() bool { return len(got()) == 1 })
	if string(got()[0]) != "hello through the board" {
		t.Fatalf("got %q", got()[0])
	}
}

// An older server sends the snapshot at once and no participant-connected:
// the snapshot alone confirms the join.
func TestBoardsJoinsOnSnapshotFirst(t *testing.T) {
	f := newFakeSocketBoard()
	f.snapshot = "first"
	host := startFakeBoard(t, f)
	a := joinFakeBoard(t, host, "guest_aaaaaa")
	waitFor(t, 3*time.Second, "carrier up", a.IsConnected)
	s := a.session.Load()
	if s.Info.session != "sess" {
		t.Fatalf("session from the snapshot: %q", s.Info.session)
	}
	if got := *s.creatorHash.Load(); got != fmt.Sprintf("%032x", 1) {
		t.Fatalf("creatorHash from the snapshot: %q", got)
	}
}

// A join sweeps the packets earlier runs left on the board (here a slow
// snapshot, as live) and leaves anything else on it alone; drops go by hash
// only, in batches.
func TestBoardsSweepsLeftoverPackets(t *testing.T) {
	f := newFakeSocketBoard()
	f.snapDelay = 300 * time.Millisecond
	const stale = 2*boardsDropBatch + 17
	for i := 0; i < stale; i++ {
		f.put(fmt.Sprintf("%032x", 1000+i), boardsPacketStyle)
	}
	f.put("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "rounded=1;whiteSpace=wrap;html=1;") // a drawing, not ours
	host := startFakeBoard(t, f)
	a := joinFakeBoard(t, host, "guest_aaaaaa")
	waitFor(t, 3*time.Second, "carrier up", a.IsConnected)
	waitFor(t, 5*time.Second, "leftovers swept", func() bool { return f.count() == 1 })
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.objects["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]; !ok {
		t.Fatal("the sweep dropped an object that is not a packet")
	}
	if f.dropEvents > stale/boardsDropBatch+1 {
		t.Fatalf("%d drop events for %d objects: not batched", f.dropEvents, stale)
	}
	for _, e := range f.dropEntries {
		var m map[string]interface{}
		_ = json.Unmarshal(e, &m)
		if len(m) != 1 || m["hash"] == nil {
			t.Fatalf("drop entry %s: want the hash alone", e)
		}
	}
}

// The receiver drops what it got, by hash and in batches, so the board stays
// empty while packets flow - and sends no packet back.
func TestBoardsReceiverDropsByHash(t *testing.T) {
	f := newFakeSocketBoard()
	host := startFakeBoard(t, f)
	a := joinFakeBoard(t, host, "guest_aaaaaa")
	b := joinFakeBoard(t, host, "guest_bbbbbb")
	waitFor(t, 3*time.Second, "both up", func() bool { return a.IsConnected() && b.IsConnected() })
	got, _ := boardsCollect(b)
	const n = 60
	payload := bytes.Repeat([]byte{0xAB}, 900)
	for i := 0; i < n; i++ {
		if err := a.Send(payload); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 5*time.Second, "all delivered", func() bool { return len(got()) == n })
	waitFor(t, 5*time.Second, "board empty", func() bool { return f.count() == 0 })
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dropEvents >= n/2 {
		t.Fatalf("%d drop events for %d packets: not batched", f.dropEvents, n)
	}
	for _, e := range f.dropEntries {
		if bytes.Contains(e, []byte("value")) {
			t.Fatalf("a drop carried the packet back: %s", e)
		}
	}
}

// A side alone on the board (an exit pinging a carrier with no client on it)
// drops its own objects once nobody has for boardsOwnTTL: they used to stay
// forever, ~8 600 a day.
func TestBoardsDropsOwnObjectsNobodyTook(t *testing.T) {
	prev := boardsOwnTTL
	boardsOwnTTL = 200 * time.Millisecond
	t.Cleanup(func() { boardsOwnTTL = prev })
	f := newFakeSocketBoard()
	host := startFakeBoard(t, f)
	a := joinFakeBoard(t, host, "guest_aaaaaa")
	waitFor(t, 3*time.Second, "carrier up", a.IsConnected)
	for i := 0; i < 20; i++ {
		if err := a.Send([]byte("ping")); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 3*time.Second, "objects on the board", func() bool { return f.count() == 20 })
	waitFor(t, 4*time.Second, "own objects dropped", func() bool { return f.count() == 0 })
	a.ownMu.Lock()
	left := len(a.own)
	a.ownMu.Unlock()
	if left != 0 {
		t.Fatalf("%d own objects still tracked", left)
	}
}

// Objects the peer dropped are not dropped again by their sender.
func TestBoardsForgetsOwnObjectsThePeerDropped(t *testing.T) {
	prev := boardsOwnTTL
	boardsOwnTTL = 300 * time.Millisecond
	t.Cleanup(func() { boardsOwnTTL = prev })
	f := newFakeSocketBoard()
	host := startFakeBoard(t, f)
	a := joinFakeBoard(t, host, "guest_aaaaaa")
	b := joinFakeBoard(t, host, "guest_bbbbbb")
	waitFor(t, 3*time.Second, "both up", func() bool { return a.IsConnected() && b.IsConnected() })
	got, _ := boardsCollect(b)
	for i := 0; i < 10; i++ {
		_ = a.Send([]byte("data"))
	}
	waitFor(t, 3*time.Second, "delivered", func() bool { return len(got()) == 10 })
	waitFor(t, 3*time.Second, "board empty", func() bool { return f.count() == 0 })
	time.Sleep(1500 * time.Millisecond) // past the TTL and a janitor round
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.dropEntries) != 10 {
		t.Fatalf("%d drops for 10 objects: the sender dropped what the peer had", len(f.dropEntries))
	}
}

// With the socket gone, Send says so (the batching layer above holds the
// packets) instead of queueing them for a socket that is never coming back.
func TestBoardsSendWhileDown(t *testing.T) {
	f := newFakeSocketBoard()
	host := startFakeBoard(t, f)
	a := joinFakeBoard(t, host, "guest_aaaaaa")
	waitFor(t, 3*time.Second, "carrier up", a.IsConnected)
	f.mu.Lock()
	f.refuse = true
	f.mu.Unlock()
	for _, p := range f.peers() {
		_ = p.ws.Close()
	}
	waitFor(t, 3*time.Second, "carrier down", func() bool { return !a.IsConnected() })
	if err := a.Send([]byte("x")); !errors.Is(err, errBoardsNotConnected) {
		t.Fatalf("Send while down: %v", err)
	}
}

// A socket.io DISCONNECT ("41") from the server is acted on: the carrier
// reconnects instead of sitting on a socket that delivers nothing.
func TestBoardsReconnectsOnNamespaceDisconnect(t *testing.T) {
	f := newFakeSocketBoard()
	host := startFakeBoard(t, f)
	a := joinFakeBoard(t, host, "guest_aaaaaa")
	waitFor(t, 3*time.Second, "carrier up", a.IsConnected)
	for _, p := range f.peers() {
		p.send("41")
	}
	waitFor(t, 5*time.Second, "second socket", func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.sockets >= 2
	})
	waitFor(t, 5*time.Second, "carrier up again", a.IsConnected)
}

func TestBoardsPaceBudget(t *testing.T) {
	now := time.Unix(1000, 0)
	p := newBoardsPace(10, 5)
	for i := 0; i < 5; i++ {
		if w := p.take(now, 0); w != 0 {
			t.Fatalf("take %d of the burst: wait %v", i, w)
		}
	}
	if w := p.take(now, 0); w != 100*time.Millisecond {
		t.Fatalf("over the burst: wait %v, want 100ms", w)
	}
	if w := p.take(now.Add(100*time.Millisecond), 0); w != 0 {
		t.Fatalf("after the refill: wait %v", w)
	}
	// The writer leaves the drops a reserve.
	p = newBoardsPace(10, 5)
	for i := 0; i < 2; i++ {
		if w := p.take(now, 3); w != 0 {
			t.Fatalf("take %d above the reserve: wait %v", i, w)
		}
	}
	if w := p.take(now, 3); w != 100*time.Millisecond {
		t.Fatalf("into the reserve: wait %v, want 100ms", w)
	}
	if w := p.take(now, 0); w != 0 {
		t.Fatal("the reserve is not there for a drop")
	}
}

// Against a server that refuses events over its rate (as the live one does,
// RATE_LIMITED), a paced carrier loses nothing; an unpaced one would.
func TestBoardsPacingKeepsUnderTheServerLimit(t *testing.T) {
	run := func(t *testing.T, rate, burst float64) (refused, delivered int, took time.Duration) {
		prevR, prevB := boardsEventRate, boardsEventBurst
		boardsEventRate, boardsEventBurst = rate, burst
		t.Cleanup(func() { boardsEventRate, boardsEventBurst = prevR, prevB })
		f := newFakeSocketBoard()
		f.limitRate, f.limitBurst = 50, 20
		host := startFakeBoard(t, f)
		a := joinFakeBoard(t, host, "guest_aaaaaa")
		b := joinFakeBoard(t, host, "guest_bbbbbb")
		waitFor(t, 3*time.Second, "both up", func() bool { return a.IsConnected() && b.IsConnected() })
		got, _ := boardsCollect(b)
		const n = 120
		began := time.Now()
		for i := 0; i < n; i++ {
			for a.Send([]byte(fmt.Sprintf("packet %d", i))) != nil {
				time.Sleep(time.Millisecond)
			}
		}
		deadline := time.Now().Add(8 * time.Second)
		for len(got()) < n && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		took = time.Since(began)
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.rateLimited, len(got()), took
	}
	t.Run("paced", func(t *testing.T) {
		refused, delivered, took := run(t, 40, 10)
		if refused != 0 || delivered != 120 {
			t.Fatalf("paced: %d refused, %d/120 delivered", refused, delivered)
		}
		if took < 2*time.Second {
			t.Fatalf("120 events at 40/s took %v: not paced", took)
		}
	})
	t.Run("unpaced", func(t *testing.T) {
		refused, delivered, _ := run(t, 100000, 100000)
		if refused == 0 || delivered == 120 {
			t.Fatalf("the fake limit did not bite: %d refused, %d delivered", refused, delivered)
		}
	})
}

func TestParseBoardsSnapshot(t *testing.T) {
	body := `[{"participant":{"socket":"s","name":"guest_1","session":"","participantHash":"ph","dashboard":"d1",` +
		`"participant":{"hash":"inner"},"device":{"screen":"1 x 1"}},` +
		`"cursor_policy":{"x":[1,2,{"y":null}]},` +
		`"objects":[` +
		`{"_attributes_":{"id":"a","style":"` + boardsPacketStyle + `","value":"QUJD"},"hash":"a","mxGeometry":[]},` +
		`{"_attributes_":{"id":"b","style":"rounded=1;"},"hash":"b"},` +
		`{"_attributes_":{"id":"c","style":"` + boardsPacketStyle + `"},"hash":"c"}],` +
		`"dialogs":[]}]`
	snap, err := parseBoardsSnapshot(strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if snap.objects != 3 || strings.Join(snap.stale, ",") != "a,c" {
		t.Fatalf("objects=%d stale=%v", snap.objects, snap.stale)
	}
	if snap.Participant.ParticipantHash != "ph" || snap.Participant.Dashboard != "d1" {
		t.Fatalf("participant: %+v", snap.Participant)
	}

	snap, err = parseBoardsSnapshot(strings.NewReader(`[{"participant":{"participantHash":"p"},"objects":null}]`))
	if err != nil || snap.objects != 0 || snap.Participant.ParticipantHash != "p" {
		t.Fatalf("null objects: %v %+v", err, snap)
	}
	snap, err = parseBoardsSnapshot(strings.NewReader(`[]`))
	if err != nil || snap.objects != 0 {
		t.Fatalf("empty: %v %+v", err, snap)
	}
	// Cut short: an error, with what was read before it.
	snap, err = parseBoardsSnapshot(strings.NewReader(`[{"objects":[{"_attributes_":{"style":"` + boardsPacketStyle + `"},"hash":"a"},{"_attr`))
	if err == nil || len(snap.stale) != 1 {
		t.Fatalf("truncated: %v %+v", err, snap)
	}
}

// readFrame takes a big snapshot that streams in for longer than the idle
// limit (but never stalls that long), and gives up on one that stalls.
func TestReadFrameStreamsBigSnapshot(t *testing.T) {
	const objects = 3000
	var stall atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up := websocket.Upgrader{}
		ws, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		wr, _ := ws.NextWriter(websocket.TextMessage)
		_, _ = wr.Write([]byte(`431[{"participant":{"participantHash":"p"},"objects":[`))
		for i := 0; i < objects; i++ {
			if i > 0 {
				_, _ = wr.Write([]byte(","))
			}
			fmt.Fprintf(wr, `{"_attributes_":{"style":%q,"value":"%s"},"hash":"%032x"}`, boardsPacketStyle, strings.Repeat("A", 200), i)
			if i%100 == 99 {
				time.Sleep(25 * time.Millisecond) // ~750 ms in all, never 25 ms idle more than once in a row
			}
			if stall.Load() && i == objects/2 {
				time.Sleep(time.Second)
			}
		}
		_, _ = wr.Write([]byte(`],"dialogs":[]}]`))
		_ = wr.Close()
		_, _, _ = ws.ReadMessage() // hold the socket until the client goes
	}))
	defer srv.Close()
	dial := func() *websocket.Conn {
		c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}

	c := dial()
	began := time.Now()
	data, snap, err := readFrame(c, time.Second, 300*time.Millisecond)
	c.Close()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if data != nil || snap == nil || len(snap.stale) != objects || snap.Participant.ParticipantHash != "p" {
		t.Fatalf("snapshot: data=%d snap=%+v", len(data), snap)
	}
	if took := time.Since(began); took < 300*time.Millisecond {
		t.Fatalf("took %v: the test did not stream past the idle limit", took)
	}
	if snap.size < objects*200 {
		t.Fatalf("size %d", snap.size)
	}

	stall.Store(true)
	c = dial()
	defer c.Close()
	if _, _, err := readFrame(c, time.Second, 300*time.Millisecond); err == nil {
		t.Fatal("a snapshot that stalls past the idle limit was read")
	}
}
