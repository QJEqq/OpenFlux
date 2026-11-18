package mailru

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/p1neappleXpress/OpenFlux/transport"
)

// fakeR7 is the Mail.ru document editor reduced to what the carrier uses — the
// Engine.IO / Socket.IO handshake, the editor auth, the chat broadcast — with the
// limits the live server keeps on each connection (see mailruConnBudget): the chat
// text it sends in all, and a message allowance. A connection over either is told
// disconnectReason 4004 and closed, as live.
type fakeR7 struct {
	srv    *httptest.Server
	budget int                 // chat text a connection may send
	burst  float64             // messages at once
	rate   float64             // messages a second after the burst
	refuse func(dial int) bool // answer this dial (1-based) with 429

	// order makes the editor take one message at a time, across connections,
	// as the document's server does: a message is broadcast before the next one
	// is taken.
	order sync.Mutex

	mu    sync.Mutex
	conns map[*fakeR7Conn]bool
	dials int
	kicks int
	most  int // the most text one connection sent
}

type fakeR7Conn struct {
	ws     *websocket.Conn
	wmu    sync.Mutex
	id     string
	text   int
	tokens float64
	at     time.Time
}

func newFakeR7(t *testing.T) *fakeR7 {
	f := &fakeR7{budget: mailruConnBudget, burst: 20, rate: 1, conns: map[*fakeR7Conn]bool{}}
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.dials++
		n := f.dials
		refused := f.refuse != nil && f.refuse(n)
		f.mu.Unlock()
		if refused {
			http.Error(w, "too many requests", http.StatusTooManyRequests)
			return
		}
		ws, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		f.serve(&fakeR7Conn{ws: ws, id: fmt.Sprintf("uidACC%d", n), tokens: f.burst, at: time.Now()}, n)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (c *fakeR7Conn) write(s string) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.ws.WriteMessage(websocket.TextMessage, []byte(s))
}

func (f *fakeR7) serve(c *fakeR7Conn, n int) {
	defer c.ws.Close()
	sid := fmt.Sprintf("sid%d", n)
	if c.write(`0{"sid":"e`+sid+`","upgrades":[],"pingInterval":25000,"pingTimeout":20000}`) != nil {
		return
	}
	if _, m, err := c.ws.ReadMessage(); err != nil || !strings.HasPrefix(string(m), "40") {
		return
	}
	if c.write(`40{"sid":"`+sid+`"}`) != nil {
		return
	}
	f.mu.Lock()
	f.conns[c] = true
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		delete(f.conns, c)
		f.mu.Unlock()
	}()
	for {
		_, m, err := c.ws.ReadMessage()
		if err != nil {
			return
		}
		text := string(m)
		switch {
		case strings.Contains(text, `"type":"auth"`):
			_ = c.write(fmt.Sprintf(`42["message",{"type":"auth","result":1,"participants":[{"id":%q,"connectionId":%q}]}]`, c.id, sid))
		case strings.Contains(text, `"type":"message"`):
			f.order.Lock()
			if !f.take(c, len(m)) {
				f.order.Unlock()
				_ = c.write(`42["message",{"type":"disconnectReason","code":4004,"description":"access deny"}]`)
				return
			}
			var frame []json.RawMessage
			var chat struct {
				Message string `json:"message"`
			}
			if json.Unmarshal(m[2:], &frame) == nil && len(frame) == 2 && json.Unmarshal(frame[1], &chat) == nil {
				out, _ := json.Marshal([]any{"message", map[string]any{"type": "message",
					"messages": []map[string]string{{"message": chat.Message, "user": c.id, "useridoriginal": "uidACC"}}}})
				f.broadcast("42" + string(out))
			}
			f.order.Unlock()
		}
	}
}

// take spends a message of n bytes of c's allowances; false when c goes over one.
func (f *fakeR7) take(c *fakeR7Conn, n int) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	c.tokens = min(f.burst, c.tokens+now.Sub(c.at).Seconds()*f.rate)
	c.at = now
	c.text += n
	c.tokens--
	if c.text > f.budget || c.tokens < 0 {
		f.kicks++
		return false
	}
	f.most = max(f.most, c.text)
	return true
}

func (f *fakeR7) broadcast(frame string) {
	f.mu.Lock()
	cs := make([]*fakeR7Conn, 0, len(f.conns))
	for c := range f.conns {
		cs = append(cs, c)
	}
	f.mu.Unlock()
	for _, c := range cs {
		_ = c.write(frame)
	}
}

func (f *fakeR7) info(string) (MailruDocsInfo, error) {
	return MailruDocsInfo{WsURL: "ws" + strings.TrimPrefix(f.srv.URL, "http") + "/doc", Token: "tok", DocKey: "doc", EditorUserID: "uidACC"}, nil
}

func (f *fakeR7) stats() (dials, kicks, most int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dials, f.kicks, f.most
}

// carrier is a transport on f, started and connected, with no gap between
// messages (the server's allowance is the only pace).
func (f *fakeR7) carrier(t *testing.T, got chan []byte) *MailruDocsTransport {
	t.Helper()
	tr := NewMailruDocsTransport("Ab/Cd", transport.DefaultConfig())
	tr.docInfo = f.info
	tr.msgInterval = 0
	tr.dialBurst, tr.dialRate = 100, 100 // the dial pace has its own test
	if got != nil {
		tr.Receive(func(p []byte) { got <- append([]byte(nil), p...) })
	}
	if err := tr.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tr.Stop() })
	deadline := time.Now().Add(10 * time.Second)
	for !tr.IsConnected() {
		if time.Now().After(deadline) {
			t.Fatal("the carrier did not connect to the fake editor")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return tr
}

// numbered makes n frames of size bytes, each starting with its number.
func numbered(n, size int) [][]byte {
	frames := make([][]byte, n)
	for i := range frames {
		frames[i] = make([]byte, size)
		_, _ = rand.Read(frames[i])
		binary.BigEndian.PutUint32(frames[i], uint32(i))
	}
	return frames
}

// pass hands frames to from the way the batcher does (a full write queue is
// tried again a moment later) and checks that they all arrive on got, in order.
func pass(t *testing.T, from *MailruDocsTransport, got chan []byte, frames [][]byte, within time.Duration) {
	t.Helper()
	sent := make(chan error, 1)
	go func() {
		deadline := time.Now().Add(within)
		for _, fr := range frames {
			for from.Send(fr) != nil {
				if time.Now().After(deadline) {
					sent <- fmt.Errorf("the carrier stopped taking frames")
					return
				}
				time.Sleep(time.Millisecond)
			}
		}
		sent <- nil
	}()
	timeout := time.After(within)
	for i := range frames {
		select {
		case p := <-got:
			if !bytes.Equal(p, frames[i]) {
				t.Fatalf("frame %d: got frame %d (%d bytes)", i, binary.BigEndian.Uint32(p), len(p))
			}
		case <-timeout:
			t.Fatalf("frame %d of %d did not arrive in %v", i, len(frames), within)
		}
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
}

// 1.25 MiB in 32 KiB frames is five connections' worth of chat text and more:
// the sender must move to fresh connections before each is spent, and the
// server must never have to cut one.
func TestCarrierStaysInsideTheConnectionBudget(t *testing.T) {
	f := newFakeR7(t)
	got := make(chan []byte, 64)
	f.carrier(t, got)
	a := f.carrier(t, nil)

	pass(t, a, got, numbered(40, 32<<10), 30*time.Second)
	dials, kicks, most := f.stats()
	if kicks != 0 {
		t.Fatalf("the server cut %d connection(s) for going over a limit", kicks)
	}
	if most > mailruSendStop {
		t.Fatalf("a connection sent %d bytes of text, over the stop of %d", most, mailruSendStop)
	}
	// 40 messages of ~43.7 KB of text are 5 to a connection: 8 for the sender, 1 for the receiver.
	if dials < 9 {
		t.Fatalf("%d dials: the sender did not move to fresh connections", dials)
	}
}

// Small messages spend the message allowance before the text: the sender moves
// to a fresh connection when it runs low instead of going over it.
func TestCarrierKeepsToTheMessageAllowance(t *testing.T) {
	f := newFakeR7(t)
	got := make(chan []byte, 128)
	f.carrier(t, got)
	a := f.carrier(t, nil)

	began := time.Now()
	pass(t, a, got, numbered(60, 100), 30*time.Second)
	_, kicks, _ := f.stats()
	if kicks != 0 {
		t.Fatalf("the server cut %d connection(s) for going over a limit", kicks)
	}
	// At one message a second after the burst one connection would take ~50 s.
	if took := time.Since(began); took > 15*time.Second {
		t.Fatalf("60 small messages took %v: the sender waited out the allowance instead of moving on", took)
	}
}

// A refused standby dial (the server's 429) must not make the sender spend the
// connection past its budget: it holds the message until a later dial gets through.
func TestCarrierWaitsForTheStandbyInsteadOfOverspending(t *testing.T) {
	f := newFakeR7(t)
	got := make(chan []byte, 64)
	f.carrier(t, got) // dial 1
	a := f.carrier(t, nil)
	f.mu.Lock()
	f.refuse = func(n int) bool { return n == 3 } // dial 2 was the sender's own: refuse its first standby
	f.mu.Unlock()

	pass(t, a, got, numbered(12, 32<<10), 30*time.Second)
	dials, kicks, _ := f.stats()
	if kicks != 0 {
		t.Fatalf("the server cut %d connection(s): the sender did not wait for the standby", kicks)
	}
	if dials < 4 {
		t.Fatalf("%d dials: the refused standby was not dialed again", dials)
	}
}

// Standby dials refused three times in a row back off 2, 4 and 8 s; the sender
// holds its message all the while and goes on, uncut, once one gets through.
func TestCarrierRidesOutRefusedStandbyDials(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out ~14 s of dial backoff")
	}
	f := newFakeR7(t)
	got := make(chan []byte, 64)
	f.carrier(t, got) // dial 1
	a := f.carrier(t, nil)
	f.mu.Lock()
	f.refuse = func(n int) bool { return n >= 3 && n < 6 }
	f.mu.Unlock()

	pass(t, a, got, numbered(8, 32<<10), 40*time.Second)
	dials, kicks, _ := f.stats()
	if kicks != 0 {
		t.Fatalf("the server cut %d connection(s): the sender did not hold for the standby", kicks)
	}
	if dials < 6 {
		t.Fatalf("%d dials: the refused standby dials were not tried again", dials)
	}
}

// The standby dials keep to the pace: a burst, then dialRate a second — the
// sender waits for a dial rather than meet the editor's 429.
func TestCarrierPacesItsDials(t *testing.T) {
	f := newFakeR7(t)
	got := make(chan []byte, 64)
	f.carrier(t, got)
	tr := NewMailruDocsTransport("Ab/Cd", transport.DefaultConfig())
	tr.docInfo = f.info
	tr.msgInterval = 0
	tr.dialBurst, tr.dialRate = 2, 2
	if err := tr.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tr.Stop() })
	for !tr.IsConnected() {
		time.Sleep(5 * time.Millisecond)
	}
	before, _, _ := f.stats()
	began := time.Now()
	pass(t, tr, got, numbered(40, 32<<10), 30*time.Second) // 8 connections' worth: 7 standby dials
	took := time.Since(began)
	dials, kicks, _ := f.stats()
	if kicks != 0 {
		t.Fatalf("the server cut %d connection(s)", kicks)
	}
	if allowed := 2 + 2*took.Seconds() + 1; float64(dials-before) > allowed {
		t.Fatalf("%d standby dials in %v, over the pace's %.1f", dials-before, took.Round(time.Millisecond), allowed)
	}
	if took < 2*time.Second {
		t.Fatalf("8 connections in %v: the dials were not paced", took)
	}
}
