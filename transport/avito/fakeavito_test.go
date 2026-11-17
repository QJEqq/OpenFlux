package avito

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// fakeAvito is an in-process code.avito.ru: room creation and a ShareDB that
// keeps one textv1 document per room, checks each op's version, acks it
// after opDelay (the real server takes a few hundred milliseconds) and
// broadcasts it to the room's other subscribers.
type fakeAvito struct {
	srv     *httptest.Server
	opDelay time.Duration

	mu    sync.Mutex
	docs  map[string]*fakeDoc
	conns map[*fakeConn]bool
	// idle closes a socket that sent nothing - no message, no ping - for
	// this long, as Avito's front does after 60 s. 0: never.
	idle time.Duration
	// readLimit > 0 closes a socket whose message is bigger (1009), as
	// Avito does above its op size.
	readLimit int64
	// opRate > 0 is the address's budget of ops a second (opBurst at once);
	// an op over it is refused with ERR_ACTION_RATE_LIMITED, not applied.
	opRate, opBurst float64
	opTokens        float64
	opLast          time.Time
	rateLimited     int
	// roomOpLimit > 0 refuses a room's ops past this doc version with
	// ERR_ROOM_OPERATION_LIMIT, as Avito does to a room it has had enough of.
	roomOpLimit int
}

func (f *fakeAvito) setRoomOpLimit(n int) {
	f.mu.Lock()
	f.roomOpLimit = n
	f.mu.Unlock()
}

func (f *fakeAvito) setIdle(d time.Duration) {
	f.mu.Lock()
	f.idle = d
	f.mu.Unlock()
}

func (f *fakeAvito) setReadLimit(n int64) {
	f.mu.Lock()
	f.readLimit = n
	f.mu.Unlock()
}

func (f *fakeAvito) setOpLimit(rate, burst float64) {
	f.mu.Lock()
	f.opRate, f.opBurst = rate, burst
	f.mu.Unlock()
}

// opLimited spends an op from the address's budget; true when there is none.
func (f *fakeAvito) opLimited() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.opRate <= 0 {
		return false
	}
	now := time.Now()
	if f.opLast.IsZero() {
		f.opTokens = f.opBurst
	} else {
		f.opTokens = min(f.opBurst, f.opTokens+now.Sub(f.opLast).Seconds()*f.opRate)
	}
	f.opLast = now
	if f.opTokens < 1 {
		f.rateLimited++
		return true
	}
	f.opTokens--
	return false
}

type fakeDoc struct {
	v       int
	created bool
	subs    map[*fakeConn]bool
}

type fakeConn struct {
	id string
	mu sync.Mutex
	ws *websocket.Conn
}

func (c *fakeConn) send(v any) {
	b, _ := json.Marshal(v)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ws.WriteMessage(websocket.TextMessage, b)
}

func newFakeAvito(t *testing.T, opDelay time.Duration) *fakeAvito {
	f := &fakeAvito{opDelay: opDelay, docs: map[string]*fakeDoc{}, conns: map[*fakeConn]bool{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/web/1/code/room", func(w http.ResponseWriter, r *http.Request) {
		id := newID()
		f.mu.Lock()
		f.docs[id] = &fakeDoc{subs: map[*fakeConn]bool{}}
		f.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]any{"room": map[string]any{"id": id}, "success": true})
	})
	mux.HandleFunc("/sharedb", f.sharedb)
	f.srv = httptest.NewServer(mux)
	old := baseURL
	baseURL = f.srv.URL
	t.Setenv("AVITO_STRIPE", "")
	t.Cleanup(func() {
		baseURL = old
		f.mu.Lock()
		for c := range f.conns {
			c.ws.Close()
		}
		f.mu.Unlock()
		f.srv.Close()
	})
	return f
}

func newID() string {
	var b [16]byte
	rand.Read(b[:])
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func (f *fakeAvito) sharedb(w http.ResponseWriter, r *http.Request) {
	up := websocket.Upgrader{ReadBufferSize: 1 << 16, WriteBufferSize: 1 << 16}
	ws, err := up.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c := &fakeConn{id: newID(), ws: ws}
	f.mu.Lock()
	f.conns[c] = true
	idle, limit := f.idle, f.readLimit
	f.mu.Unlock()
	if limit <= 0 {
		limit = 16 << 20
	}
	ws.SetReadLimit(limit)
	var last atomic.Int64
	touch := func() { last.Store(time.Now().UnixNano()) }
	touch()
	if idle > 0 {
		ws.SetPingHandler(func(data string) error {
			touch()
			return ws.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(time.Second))
		})
		stop := make(chan struct{})
		defer close(stop)
		go func() {
			tick := time.NewTicker(idle / 4)
			defer tick.Stop()
			for {
				select {
				case <-stop:
					return
				case <-tick.C:
					if time.Since(time.Unix(0, last.Load())) > idle {
						ws.Close()
						return
					}
				}
			}
		}()
	}
	defer func() {
		f.mu.Lock()
		delete(f.conns, c)
		for _, d := range f.docs {
			delete(d.subs, c)
		}
		f.mu.Unlock()
		ws.Close()
	}()
	c.send(map[string]any{"type": "init", "id": c.id})
	for {
		_, raw, err := ws.ReadMessage()
		if err != nil {
			return
		}
		touch()
		var m map[string]any
		if json.Unmarshal(raw, &m) != nil {
			continue
		}
		room, _ := m["d"].(string)
		switch m["a"] {
		case "s":
			f.mu.Lock()
			d := f.docs[room]
			if d == nil {
				f.mu.Unlock()
				c.send(map[string]any{"a": "s", "c": "docs", "d": room, "error": map[string]any{"code": 404}})
				continue
			}
			d.subs[c] = true
			doc := map[string]any{"v": d.v, "data": ""}
			if d.created {
				doc["type"] = shareDBType
			} else {
				doc["type"] = nil
			}
			f.mu.Unlock()
			c.send(map[string]any{"a": "s", "c": "docs", "d": room, "data": doc})
		case "op":
			if f.opLimited() {
				c.send(map[string]any{"a": "op", "c": "docs", "d": room, "src": m["src"], "seq": m["seq"],
					"error": map[string]any{"code": "ERR_ACTION_RATE_LIMITED", "message": "ShareDB action rate limited"}})
				continue
			}
			time.Sleep(f.opDelay)
			v, _ := m["v"].(float64)
			f.mu.Lock()
			d := f.docs[room]
			if d != nil && f.roomOpLimit > 0 && d.v >= f.roomOpLimit {
				f.mu.Unlock()
				c.send(map[string]any{"a": "op", "c": "docs", "d": room, "src": m["src"], "seq": m["seq"],
					"error": map[string]any{"code": "ERR_ROOM_OPERATION_LIMIT", "message": "Room operation limit exceeded"}})
				continue
			}
			if d == nil || int(v) != d.v {
				f.mu.Unlock()
				c.send(map[string]any{"a": "op", "c": "docs", "d": room, "src": m["src"], "seq": m["seq"],
					"error": map[string]any{"code": "ERR_OP_VERSION"}})
				continue
			}
			if _, ok := m["create"]; ok {
				d.created = true
			}
			d.v++
			var others []*fakeConn
			for s := range d.subs {
				if s != c {
					others = append(others, s)
				}
			}
			f.mu.Unlock()
			c.send(map[string]any{"a": "op", "c": "docs", "d": room, "src": m["src"], "seq": m["seq"], "v": v})
			if op, ok := m["op"]; ok {
				for _, s := range others {
					s.send(map[string]any{"a": "op", "c": "docs", "d": room, "src": m["src"], "seq": m["seq"], "v": v, "op": op})
				}
			}
		}
	}
}

// dropConns hangs up every socket subscribed to room.
func (f *fakeAvito) dropConns(room string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if d := f.docs[room]; d != nil {
		for c := range d.subs {
			c.ws.Close()
		}
	}
}
