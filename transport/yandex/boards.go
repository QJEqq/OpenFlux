package yandex

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	mrand "math/rand"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/p1neappleXpress/OpenFlux/transport"
	"github.com/p1neappleXpress/OpenFlux/utils"
)

const (
	boardsBase = "boards.yandex.ru"
	boardsUA   = "Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 " +
		"(KHTML, like Gecko) Chrome/153.0.0.0 Mobile Safari/537.36"
	boardsSocketHostDefault = "socket33.boards.yandex.ru"

	// Heartbeat. Сервер шлёт engine.io ping "2" (pingInterval=25000,
	// pingTimeout=30000), мы отвечаем "3". Сами шлём только heartbeat в
	// namespace dashboard каждые 20с, чтобы NAT не рвал idle-соединение;
	// клиентский "2" сервер EIO=4 считает ошибкой и закрывает сокет.
	boardsPingInterval = 20 * time.Second

	// How long a socket may go without a byte arriving, well above the
	// server's 25 s ping. It is idle time, not a bound on a whole message: a
	// frame keeps the socket alive while it streams in, however big (the
	// join snapshot holds every object on the board).
	boardsReadDeadline = 90 * time.Second

	// How long the server has to confirm a subscription.
	boardsHandshakeWait = 15 * time.Second

	// boardsPacketStyle is the style of the text objects that carry packets;
	// a board's objects of any other style are not ours and are left alone.
	boardsPacketStyle = "text;html=1;strokeColor=none;fillColor=none;align=left;verticalAlign=middle;whiteSpace=wrap;rounded=0;fontSize=1;"

	// Board cleanup. A packet object stays on the board until a participant
	// drops it, and every participant that joins is sent the whole board
	// (the snapshot, ack 431). Live, an exit that kept pinging a carrier with
	// no client on it left ~8 600 objects a day nobody dropped; after a week
	// the snapshot was 44 MB and took longer than a phone's join could wait.
	// So the objects a side receives are dropped in batches, by hash alone
	// (the server needs nothing else, and sending a packet's value back made
	// a receiver upload as much as it downloaded), its own objects that the
	// peer has not dropped within boardsOwnTTL are dropped by itself, and
	// what a snapshot shows of earlier packets is swept on join.
	boardsDropTick  = 150 * time.Millisecond
	boardsDropBatch = 300 // hashes per drop-objects: ~13 KB; the server refuses a message above ~45 KB (PAYLOAD_TOO_LARGE)
	boardsDropBurst = 2   // drop-objects events per tick: a sweep of 60 000 objects takes ~15 s
	boardsOwnMax    = 100000

	// boardsBatchBytes is the batch the layer above should fill one event
	// with (transport.BatchSizeHinter). Events, not bytes, are what the
	// board limits (see boardsEventRate); 16 KB is ~22 KB of base64, half
	// the largest message the server takes.
	boardsBatchBytes = 16 << 10

	// boardsQueueEvents is how many events may wait for the socket's writer.
	boardsQueueEvents = 64
)

// The board answers RATE_LIMITED - and drops the event - once a participant
// keeps much above ~100 events a second (live 2026-11-17: 160/s held for
// 5 s, then refusals from ~1 500 events on; ~98/s got through on average).
// A socket's modify-objects and drop-objects are paced below that, with
// room for a burst (vars for tests).
var (
	boardsEventRate  = 60.0
	boardsEventBurst = 200.0
)

// boardsPace is a socket's event budget: a token bucket.
type boardsPace struct {
	mu          sync.Mutex
	rate, burst float64
	tokens      float64
	last        time.Time
}

func newBoardsPace(rate, burst float64) *boardsPace {
	return &boardsPace{rate: rate, burst: burst, tokens: burst}
}

// boardsPaceReserve is what data leaves in the budget for the drops, so a
// socket saturated with data still clears the board as it goes.
const boardsPaceReserve = 3

// take spends one event if the budget holds more than reserve beyond it
// (0), or says how long until it will.
func (p *boardsPace) take(now time.Time, reserve float64) time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.last.IsZero() {
		p.tokens = min(p.burst, p.tokens+now.Sub(p.last).Seconds()*p.rate)
	}
	p.last = now
	if p.tokens >= 1+reserve {
		p.tokens--
		return 0
	}
	return time.Duration((1 + reserve - p.tokens) / p.rate * float64(time.Second))
}

// boardsOwnTTL is how long an object of ours may wait for the peer to drop
// it before we drop it ourselves (a var for tests).
var boardsOwnTTL = 10 * time.Second

// boardsReauthAfter is how many short-lived connections in a row make the
// carrier sign in to the board again (a fresh guest token, maybe another
// socket server) instead of retrying the old one (a var for tests).
var boardsReauthAfter = 4

// boardsTLS is the TLS configuration of the board socket; nil is the
// default. Tests point it at their server.
var boardsTLS *tls.Config

// ---- внутренние типы ----

type boardsInfo struct {
	hash         string
	name         string
	userHash     string // payload.u из JWT
	jwt          string
	cookies      []*http.Cookie
	wsHost       string
	session      string // пустой до participant-connected/431
	dashboard    string
	currentSlide string
}

type boardsSession struct {
	Info    boardsInfo
	Conn    *websocket.Conn
	Queue   chan []byte
	writeMu sync.Mutex
	ack     atomic.Int64

	// participant — тот, что мы отправили в subscribe. Это userHash из JWT.
	participant atomic.Pointer[string]

	// creatorHash — серверный хэш (из participant-connected/431).
	creatorHash atomic.Pointer[string]

	// joined: the server confirmed the subscription (our own
	// participant-connected, or the snapshot).
	joined atomic.Bool

	// closed is closed when this socket is done: its writer and dropper stop
	// and Send stops queueing for it.
	closed    chan struct{}
	closeOnce sync.Once

	// Objects to drop from the board, in order, without repeats.
	dropMu  sync.Mutex
	dropQ   []string
	dropSet map[string]struct{}

	// pace is this socket's budget of modify-objects and drop-objects.
	pace *boardsPace
}

func newBoardsSession(info boardsInfo, conn *websocket.Conn, queue int, pace *boardsPace) *boardsSession {
	return &boardsSession{
		Info:    info,
		Conn:    conn,
		Queue:   make(chan []byte, queue),
		closed:  make(chan struct{}),
		dropSet: map[string]struct{}{},
		pace:    pace,
	}
}

func (s *boardsSession) close() { s.closeOnce.Do(func() { close(s.closed) }) }

func (s *boardsSession) isClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

// queueDrop asks for objects to be dropped from the board.
func (s *boardsSession) queueDrop(hashes ...string) {
	s.dropMu.Lock()
	defer s.dropMu.Unlock()
	for _, h := range hashes {
		if h == "" {
			continue
		}
		if _, dup := s.dropSet[h]; dup {
			continue
		}
		s.dropSet[h] = struct{}{}
		s.dropQ = append(s.dropQ, h)
	}
}

// pendingDrops is how many objects wait to be dropped.
func (s *boardsSession) pendingDrops() int {
	s.dropMu.Lock()
	defer s.dropMu.Unlock()
	return len(s.dropQ)
}

// takeDrops takes up to n objects to drop.
func (s *boardsSession) takeDrops(n int) []string {
	s.dropMu.Lock()
	defer s.dropMu.Unlock()
	n = min(n, len(s.dropQ))
	if n == 0 {
		return nil
	}
	out := append([]string(nil), s.dropQ[:n]...)
	s.dropQ = s.dropQ[n:]
	for _, h := range out {
		delete(s.dropSet, h)
	}
	if len(s.dropQ) == 0 {
		s.dropQ = nil // let a swept backlog go
	}
	return out
}

func (s *boardsSession) safeWrite(msgType int, data []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.Conn.WriteMessage(msgType, data)
}

func shortStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func (s *boardsSession) writeEventObj(ns string, obj interface{}) error {
	ack := s.ack.Add(1) - 1
	payload, err := json.Marshal([]interface{}{ns, obj})
	if err != nil {
		return err
	}
	msg := fmt.Sprintf("42%d%s", ack, payload)
	return s.safeWrite(websocket.TextMessage, []byte(msg))
}

func (s *boardsSession) writeRaw(raw string) error {
	return s.safeWrite(websocket.TextMessage, []byte(raw))
}

// ---- транспорт ----

type BoardsTransport struct {
	*transport.BaseTransport

	url string

	session atomic.Pointer[boardsSession]

	onDataMu sync.RWMutex
	onData   func([]byte)

	closeOnce sync.Once
	done      chan struct{}

	cookieJar *cookiejar.Jar
	jarMu     sync.RWMutex

	errNotifier func(err error, transportName, url, html, reason string)

	// own: the objects we put on the board that nobody has dropped yet, and
	// when. They outlive a socket: the next one drops what the last left.
	ownMu sync.Mutex
	own   map[string]time.Time

	// The package knobs, as they were when the carrier was made.
	ownTTL                time.Duration
	reauthAfter           int
	wsTLS                 *tls.Config
	eventRate, eventBurst float64
}

// PreferredBatchBytes implements transport.BatchSizeHinter: the board limits
// events, so each should carry as much as the layer above has.
func (t *BoardsTransport) PreferredBatchBytes() int { return boardsBatchBytes }

// errBoardsNotConnected is Send's answer while the board socket is down;
// the batching layer above keeps the packets and retries.
var errBoardsNotConnected = errors.New("boards: not connected")

// trackOwn notes an object of ours on the board.
func (t *BoardsTransport) trackOwn(hash string, now time.Time) {
	t.ownMu.Lock()
	defer t.ownMu.Unlock()
	if t.own == nil {
		t.own = map[string]time.Time{}
	}
	if len(t.own) < boardsOwnMax {
		t.own[hash] = now
	}
}

// forgetOwn is told the objects someone dropped.
func (t *BoardsTransport) forgetOwn(hashes []string) {
	t.ownMu.Lock()
	defer t.ownMu.Unlock()
	for _, h := range hashes {
		delete(t.own, h)
	}
}

// expiredOwn takes our objects put on the board before cutoff: the peer did
// not drop them (it is not there, or lost them), so we will.
func (t *BoardsTransport) expiredOwn(cutoff time.Time) []string {
	t.ownMu.Lock()
	defer t.ownMu.Unlock()
	var out []string
	for h, at := range t.own {
		if at.Before(cutoff) {
			out = append(out, h)
			delete(t.own, h)
		}
	}
	return out
}

func NewBoardsTransport(rawURL string, config transport.TransportConfig) *BoardsTransport {
	jar, _ := cookiejar.New(nil)
	return &BoardsTransport{
		BaseTransport: transport.NewBaseTransport(config),
		url:           rawURL,
		done:          make(chan struct{}),
		cookieJar:     jar,
		ownTTL:        boardsOwnTTL,
		reauthAfter:   boardsReauthAfter,
		wsTLS:         boardsTLS,
		eventRate:     boardsEventRate,
		eventBurst:    boardsEventBurst,
	}
}

// SetErrorNotifier installs a callback for out-of-band errors. Called once
// by the manager. Boards currently never returns sentinel errors from
// fetchDocInfo (its captcha path is the old showcaptchafast, which the
// internal PoW solver handles), but the hook is wired for parity with the
// other transports.
func (t *BoardsTransport) SetErrorNotifier(fn func(err error, transportName, url, html, reason string)) {
	t.errNotifier = fn
}

func (t *BoardsTransport) Start() error {
	if err := t.BaseTransport.Start(); err != nil {
		return err
	}
	hash := extractBoardsHash(t.url)
	if hash == "" {
		return fmt.Errorf("boards: no hash in URL %q", t.url)
	}

	name := randomGuestName()
	info, err := t.authorize(hash, name)
	if err != nil {
		return fmt.Errorf("boards auth: %w", err)
	}
	utils.Debugf("[BOARDS] auth OK: hash=%s name=%q userHash=%s dashboard=%q wsHost=%s",
		info.hash, info.name, info.userHash, info.dashboard, info.wsHost)

	t.closeOnce = sync.Once{}
	t.done = make(chan struct{})
	utils.SafeGo("boards.connect", func() { t.connectLoop(info) })

	return nil
}

func (t *BoardsTransport) Stop() error {
	t.closeOnce.Do(func() { close(t.done) })
	if s := t.session.Load(); s != nil && s.Conn != nil {
		s.Conn.Close()
	}
	t.SetConnected(false)
	return t.BaseTransport.Stop()
}

func (t *BoardsTransport) Send(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	s := t.session.Load()
	if s == nil || s.isClosed() {
		// A socket that is gone takes nothing: what was queued for it would
		// only be lost. The batching layer holds the packets until the
		// carrier is back.
		return errBoardsNotConnected
	}
	// The board's event budget is waited for here, where the layer above
	// feels it: while one event waits, what arrives meanwhile goes into the
	// next one. A TCP flow trickles in a packet at a time, and with the wait
	// further down (or none) every packet took an event of its own - 60
	// events of 1.4 KB are 84 KB/s, measured through a live tunnel.
	if !t.awaitPace(s) {
		return errBoardsNotConnected
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	select {
	case s.Queue <- cp:
		return nil
	case <-t.done:
		return fmt.Errorf("boards: closed")
	default:
		return fmt.Errorf("boards: queue full")
	}
}

func (t *BoardsTransport) Receive(cb func([]byte)) {
	t.onDataMu.Lock()
	t.onData = cb
	t.onDataMu.Unlock()
}

func (t *BoardsTransport) IsConnected() bool {
	return t.BaseTransport.IsConnected()
}

// ---- авторизация ----
//
// 1) GET /whiteboard/?hash=<hash>   → начальные cookies
//    (может редиректнуть на showcaptchafast — тогда проходим капчу)
// 2) POST /api request-guest-token  → Set-Cookie token_<hash>=<JWT>
// 3) POST /api get-whiteboard-info  → dashboard/current_slide/ws_host

// errCaptchaRequired — sentinel error: сервер требует капчу.
var errCaptchaRequired = fmt.Errorf("captcha required")

// getAllowCaptcha делает GET и, если встречает редирект на showcaptchafast,
// возвращает errCaptchaRequired.
func (t *BoardsTransport) getAllowCaptcha(client *http.Client, u, hash string) error {
	req, _ := http.NewRequest("GET", u, nil)
	req.Header.Set("User-Agent", boardsUA)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Referer", "https://"+boardsBase+"/guest/?hash="+hash)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		loc := resp.Header.Get("Location")
		if strings.Contains(loc, "showcaptchafast") {
			utils.Debugf("[BOARDS] redirect to captcha: %s", shortStr(loc, 100))
			return errCaptchaRequired
		}
	}
	io.Copy(io.Discard, resp.Body)
	return nil
}

func (t *BoardsTransport) authorize(hash, name string) (boardsInfo, error) {
	jar := t.jar()
	client := &http.Client{
		Jar:       jar,
		Transport: carrierTransport(t.network()),
		Timeout:   15 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	docURL := "https://" + boardsBase + "/whiteboard/?hash=" + hash

	// GET whiteboard — может вернуть 302 на showcaptchafast
	if err := t.getAllowCaptcha(client, docURL, hash); err != nil {
		if err == errCaptchaRequired {
			utils.Debugf("[BOARDS] captcha required, solving...")
			if _, cerr := solveCaptcha(docURL, jar, boardsUA); cerr != nil {
				return boardsInfo{}, fmt.Errorf("captcha solve: %w", cerr)
			}
			utils.Debugf("[BOARDS] captcha solved, re-fetching whiteboard")

			// После капчи повторяем GET /whiteboard — сервер выдаёт
			// свежие cookies, нужные для последующих /api запросов.
			if err := t.getAllowCaptcha(client, docURL, hash); err != nil && err != errCaptchaRequired {
				return boardsInfo{}, fmt.Errorf("GET whiteboard (post-captcha): %w", err)
			}
		} else {
			return boardsInfo{}, err
		}
	}

	if err := t.postAPI(client, hash, "request-guest-token",
		map[string]string{"name": name, "hash": hash}); err != nil {
		return boardsInfo{}, fmt.Errorf("request-guest-token: %w", err)
	}

	u, _ := url.Parse("https://" + boardsBase)
	var jwt string
	for _, c := range jar.Cookies(u) {
		if c.Name == "token_"+hash {
			jwt = c.Value
		}
	}
	if jwt == "" {
		return boardsInfo{}, fmt.Errorf("token_%s not found", hash)
	}
	payload := jwtPayload(jwt)
	userHash, _ := payload["u"].(string)

	state, err := t.getWhiteboardInfo(client, hash)
	if err != nil {
		utils.Debugf("[BOARDS] get-whiteboard-info failed: %v", err)
	}

	var cookies []*http.Cookie
	cookies = append(cookies, jar.Cookies(u)...)

	wsHost := state["ws_host"]
	if wsHost == "" {
		wsHost = boardsSocketHostDefault
	}

	return boardsInfo{
		hash:         hash,
		name:         name,
		userHash:     userHash,
		jwt:          jwt,
		cookies:      cookies,
		wsHost:       wsHost,
		session:      "",
		dashboard:    state["dashboard"],
		currentSlide: state["current_slide"],
	}, nil
}

// apiRequest builds a POST /api call: the action and its content (JSON,
// base64-encoded) in a JSON body. The API used to take a form and now
// answers 415 "Request body must use a JSON Content-Type" to one.
func apiRequest(hash, action string, content interface{}) *http.Request {
	raw, _ := json.Marshal(content)
	body, _ := json.Marshal(map[string]string{
		"action":  action,
		"content": base64.StdEncoding.EncodeToString(raw),
	})
	req, _ := http.NewRequest("POST", "https://"+boardsBase+"/api", bytes.NewReader(body))
	req.Header.Set("User-Agent", boardsUA)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Accept", "application/json, text/javascript, */*; q=0.01")
	req.Header.Set("Referer", "https://"+boardsBase+"/guest/?hash="+hash)
	req.Header.Set("Origin", "https://"+boardsBase)
	return req
}

func (t *BoardsTransport) postAPI(client *http.Client, hash, action string, content interface{}) error {
	resp, err := client.Do(apiRequest(hash, action, content))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		n := len(body)
		if n > 200 {
			n = 200
		}
		return fmt.Errorf("status %d: %s", resp.StatusCode, string(body[:n]))
	}
	return nil
}

func (t *BoardsTransport) getWhiteboardInfo(client *http.Client, hash string) (map[string]string, error) {
	resp, err := client.Do(apiRequest(hash, "get-whiteboard-info", map[string]string{"hash": hash}))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	var info struct {
		Presentation struct {
			Properties struct {
				CurrentSlide string `json:"current_slide"`
			} `json:"properties"`
			Items string `json:"items"`
		} `json:"presentation"`
		SocketServers []struct {
			IP string `json:"ip"`
		} `json:"socket_servers"`
	}
	if err := json.Unmarshal(body, &info); err != nil {
		return nil, err
	}
	out := map[string]string{}
	if info.Presentation.Properties.CurrentSlide != "" {
		out["current_slide"] = info.Presentation.Properties.CurrentSlide
		out["dashboard"] = info.Presentation.Properties.CurrentSlide
	}
	if info.Presentation.Items != "" {
		if rawItems, err := base64.StdEncoding.DecodeString(info.Presentation.Items); err == nil {
			var arr []map[string]interface{}
			if json.Unmarshal(rawItems, &arr) == nil && len(arr) > 0 {
				if h, _ := arr[0]["hash"].(string); h != "" && out["dashboard"] == "" {
					out["dashboard"] = h
					out["current_slide"] = h
				}
			}
		}
	}
	if len(info.SocketServers) > 0 {
		out["ws_host"] = info.SocketServers[0].IP
	}
	return out, nil
}

func jwtPayload(jwt string) map[string]interface{} {
	parts := strings.Split(jwt, ".")
	if len(parts) < 2 {
		return nil
	}
	pad := (4 - len(parts[1])%4) % 4
	b := parts[1] + strings.Repeat("=", pad)
	raw, err := base64.URLEncoding.DecodeString(b)
	if err != nil {
		return nil
	}
	var m map[string]interface{}
	_ = json.Unmarshal(raw, &m)
	return m
}

func extractBoardsHash(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Query().Get("hash")
}

func randomGuestName() string {
	var b [3]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("guest_%06x", time.Now().UnixNano()&0xffffff)
	}
	return "guest_" + hex.EncodeToString(b[:])
}

func randomHex(n int) string {
	buf := make([]byte, n/2)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%0*x", n, mrand.Int63())
	}
	return hex.EncodeToString(buf)
}

// ---- WS lifecycle ----

func (t *BoardsTransport) connectLoop(info boardsInfo) {
	attempt := 0
	short := 0 // connections in a row that did not hold
	for {
		select {
		case <-t.done:
			return
		default:
		}
		began := time.Now()
		err := t.connectAndServe(info)
		lasted := time.Since(began)
		if err != nil {
			utils.Debugf("[BOARDS] ws error after %v: %v", lasted.Round(time.Second), err)
			if utils.Throttled("boards.drop", time.Minute) {
				utils.Infof("[BOARDS] connection to the board dropped after %v: %v; reconnecting", lasted.Round(time.Second), err)
			}
		}
		t.SetConnected(false)
		if lasted > time.Minute {
			// A session that held is not part of a failure streak.
			attempt, short = 0, 0
		} else if short++; short >= t.reauthAfter {
			// The guest token or the socket server the sign-in named may be
			// what fails: sign in again rather than retry them forever.
			short = 0
			if fresh, err := t.authorize(info.hash, randomGuestName()); err != nil {
				utils.Debugf("[BOARDS] signing in to the board again: %v", err)
			} else {
				utils.Infof("[BOARDS] signed in to the board again (socket %s) after %d short connections", fresh.wsHost, t.reauthAfter)
				info = fresh
			}
		}
		select {
		case <-t.done:
			return
		case <-time.After(reconnectBackoffBoards(attempt)):
		}
		attempt++
		if attempt > 10 {
			attempt = 10
		}
	}
}

func reconnectBackoffBoards(n int) time.Duration {
	if n < 1 {
		n = 1
	}
	shift := n - 1
	if shift > 4 {
		shift = 4
	}
	d := 500 * time.Millisecond * time.Duration(1<<uint(shift))
	if d > 15*time.Second {
		d = 15 * time.Second
	}
	d += time.Duration(mrand.Int63n(int64(d/2) + 1))
	return d
}

func (t *BoardsTransport) connectAndServe(info boardsInfo) error {
	wsURL := fmt.Sprintf("wss://%s/socket.io/?EIO=4&transport=websocket", info.wsHost)

	header := http.Header{}
	header.Set("User-Agent", boardsUA)
	header.Set("Origin", "https://"+boardsBase)
	header.Set("Accept-Language", "en-US,en;q=0.9")

	var cookieParts []string
	for _, c := range info.cookies {
		cookieParts = append(cookieParts, c.Name+"="+c.Value)
	}
	if !strings.Contains(strings.Join(cookieParts, ";"), "token_"+info.hash) {
		cookieParts = append(cookieParts, "token_"+info.hash+"="+info.jwt)
	}
	header.Set("Cookie", strings.Join(cookieParts, "; "))

	dialer := websocket.Dialer{
		HandshakeTimeout: 15 * time.Second,
		NetDialContext:   dialIPv4FirstOn(t.network()),
		TLSClientConfig:  t.wsTLS,
	}
	utils.Debugf("[BOARDS] dial %s", wsURL)
	conn, resp, err := dialer.Dial(wsURL, header)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		return fmt.Errorf("dial %s (http %d): %w", wsURL, status, err)
	}
	utils.Debugf("[BOARDS] WS connected: %s", info.wsHost)

	participant := info.userHash
	creator := info.userHash
	// Send waits for the budget, so the queue only covers the socket's own
	// write time; a deep one would just hold seconds of data in front of
	// every flow.
	sess := newBoardsSession(info, conn, min(t.GetConfig().MaxQueueSize, boardsQueueEvents), newBoardsPace(t.eventRate, t.eventBurst))
	sess.participant.Store(&participant)
	sess.creatorHash.Store(&creator)
	t.session.Store(sess)
	defer sess.close()

	if err := t.handshake(sess); err != nil {
		conn.Close()
		t.session.CompareAndSwap(sess, nil)
		return fmt.Errorf("handshake: %w", err)
	}

	t.SetConnected(true)
	utils.SafeGo("boards.writer", func() { t.writerLoop(sess) })
	utils.SafeGo("boards.dropper", func() { t.dropLoop(sess) })

	// No client-side engine.io ping: in EIO=4 the server pings ("2") and the
	// client answers ("3", see handleMessage). A client "2" is an invalid
	// heartbeat direction to an engine.io v4 server, which then closes the
	// socket, so the old pingLoop dropped the board every
	// boardsPingInterval. The dashboard heartbeat below keeps it busy.
	utils.SafeGo("boards.keepalive", func() { t.keepAliveLoop(sess) })

	for {
		select {
		case <-t.done:
			return nil
		default:
		}

		msg, snap, err := readFrame(conn, boardsReadDeadline, boardsReadDeadline)
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		if snap != nil {
			t.handleSnapshot(sess, snap)
			continue
		}
		t.handleMessage(sess, msg)
	}
}

func (t *BoardsTransport) handshake(sess *boardsSession) error {
	conn := sess.Conn
	if _, err := readSmall(conn, 15*time.Second); err != nil {
		return fmt.Errorf("engine.io hello: %w", err)
	}

	if err := sess.writeRaw("40"); err != nil {
		return err
	}
	if _, err := readSmall(conn, 15*time.Second); err != nil {
		return fmt.Errorf("socket.io connect ack: %w", err)
	}

	if err := sess.writeEventObj("im", map[string]interface{}{
		"operation": "subscribe", "user": nil,
	}); err != nil {
		return err
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		m, err := readSmall(conn, 5*time.Second)
		if err != nil {
			return fmt.Errorf("wait subscribed: %w", err)
		}
		if bytes.Contains(m, []byte(`"subscribed"`)) {
			break
		}
	}

	part := *sess.participant.Load()
	utils.Debugf("[BOARDS] subscribe-slide-dashboard participant=%s", shortStr(part, 8))
	if err := t.sendSubscribe(sess, part); err != nil {
		return err
	}

	// Current servers confirm the subscription with our own
	// participant-connected and send the snapshot (ack 431: every object on
	// the board) when it is ready - on a board nobody had cleaned, 44 MB
	// after 25-45 s, which a join that waited for it never lived to see.
	// Older servers sent the snapshot at once. Either one confirms the
	// join; the snapshot is read when it comes.
	deadline = time.Now().Add(boardsHandshakeWait)
	for !sess.joined.Load() {
		wait := time.Until(deadline)
		if wait <= 0 {
			return fmt.Errorf("the board did not confirm the subscription in %v", boardsHandshakeWait)
		}
		m, snap, err := readFrame(conn, wait, boardsReadDeadline)
		if err != nil {
			return fmt.Errorf("wait for the subscription: %w", err)
		}
		if snap != nil {
			t.handleSnapshot(sess, snap)
			continue
		}
		t.handleMessage(sess, m)
		if bytes.Contains(m, []byte(`"subscribed":true`)) &&
			bytes.Contains(m, []byte(`"dashboard_link"`)) {
			sess.joined.Store(true)
		}
	}

	utils.Debugf("[BOARDS] handshake done")
	return nil
}

// readSmall reads one message expected within wait (handshake steps).
func readSmall(conn *websocket.Conn, wait time.Duration) ([]byte, error) {
	m, _, err := readFrame(conn, wait, wait)
	return m, err
}

// readFrame reads one socket message: wait bounds the time until it starts,
// idle the time between its bytes. The join snapshot (ack 431, every object
// on the board) is parsed as it streams in and comes back as snap, with no
// bytes: a board in a bad state makes it tens of megabytes, which a phone
// should not hold in memory, nor lose the socket over.
func readFrame(conn *websocket.Conn, wait, idle time.Duration) ([]byte, *boardsSnapshot, error) {
	_ = conn.SetReadDeadline(time.Now().Add(wait))
	_, r, err := conn.NextReader()
	if err != nil {
		return nil, nil, err
	}
	ir := &idleReader{conn: conn, r: r, idle: idle}
	br := bufio.NewReaderSize(ir, 32<<10)
	if head, _ := br.Peek(4); bytes.Equal(head, []byte("431[")) {
		_, _ = br.Discard(3)
		snap, perr := parseBoardsSnapshot(br)
		if _, err := io.Copy(io.Discard, br); err != nil {
			return nil, nil, err
		}
		if perr != nil {
			utils.Debugf("[BOARDS] join snapshot: %v", perr)
		}
		snap.size = ir.n
		return nil, snap, nil
	}
	data, err := io.ReadAll(br)
	if err != nil {
		return nil, nil, err
	}
	return data, nil, nil
}

// idleReader renews the socket's read deadline before every read, so a big
// message keeps the socket alive while its bytes keep coming.
type idleReader struct {
	conn *websocket.Conn
	r    io.Reader
	idle time.Duration
	n    int64
}

func (ir *idleReader) Read(p []byte) (int, error) {
	_ = ir.conn.SetReadDeadline(time.Now().Add(ir.idle))
	n, err := ir.r.Read(p)
	ir.n += int64(n)
	return n, err
}

// boardsSnapshot is what the join snapshot (the ack of
// subscribe-slide-dashboard) says: who we are on the board, and the packet
// objects lying on it - each one a packet nobody dropped.
type boardsSnapshot struct {
	Participant struct {
		Session         string `json:"session"`
		ParticipantHash string `json:"participantHash"`
		CreatorHash     string `json:"creatorHash"`
		Dashboard       string `json:"dashboard"`
		DashboardLink   struct {
			Session   string `json:"session"`
			Dashboard string `json:"dashboard"`
		} `json:"dashboard_link"`
	}
	objects int      // objects on the board
	stale   []string // hashes of the packet objects among them
	size    int64    // bytes of the snapshot
}

// parseBoardsSnapshot reads a snapshot body ([{"participant":{...},
// "objects":[...], ...}]) one object at a time, so its size costs no memory.
// On an error it returns what it read so far.
func parseBoardsSnapshot(r io.Reader) (*boardsSnapshot, error) {
	snap := &boardsSnapshot{}
	dec := json.NewDecoder(r)
	if err := expectDelim(dec, '['); err != nil {
		return snap, err
	}
	if !dec.More() {
		return snap, nil
	}
	if err := expectDelim(dec, '{'); err != nil {
		return snap, err
	}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return snap, err
		}
		switch key, _ := tok.(string); key {
		case "participant":
			if err := dec.Decode(&snap.Participant); err != nil {
				return snap, err
			}
		case "objects":
			tok, err := dec.Token()
			if err != nil {
				return snap, err
			}
			if tok == nil {
				continue // null
			}
			if d, ok := tok.(json.Delim); !ok || d != '[' {
				return snap, fmt.Errorf("objects: %v", tok)
			}
			for dec.More() {
				var o struct {
					Attributes struct {
						Style string `json:"style"`
					} `json:"_attributes_"`
					Hash string `json:"hash"`
				}
				if err := dec.Decode(&o); err != nil {
					return snap, err
				}
				snap.objects++
				if o.Hash != "" && strings.HasPrefix(o.Attributes.Style, boardsPacketStyle) {
					snap.stale = append(snap.stale, o.Hash)
				}
			}
			if _, err := dec.Token(); err != nil { // ]
				return snap, err
			}
		default:
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return snap, err
			}
		}
	}
	return snap, nil
}

func expectDelim(dec *json.Decoder, want json.Delim) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if d, ok := tok.(json.Delim); !ok || d != want {
		return fmt.Errorf("want %q, got %v", want, tok)
	}
	return nil
}

// handleSnapshot takes what the join snapshot says: the subscription is
// confirmed, who we are, and the packets left on the board, which are swept.
func (t *BoardsTransport) handleSnapshot(sess *boardsSession, snap *boardsSnapshot) {
	p := snap.Participant
	if p.DashboardLink.Session != "" && sess.Info.session == "" {
		sess.Info.session = p.DashboardLink.Session
	}
	if p.Session != "" && sess.Info.session == "" {
		sess.Info.session = p.Session
	}
	dash := p.DashboardLink.Dashboard
	if dash == "" {
		dash = p.Dashboard
	}
	if dash != "" && sess.Info.dashboard == "" {
		sess.Info.dashboard = dash
		sess.Info.currentSlide = dash
		utils.Debugf("[BOARDS] dashboard from 431: %s", shortStr(dash, 8))
	}
	for _, h := range []string{p.CreatorHash, p.ParticipantHash} {
		if h != "" {
			hh := h
			sess.creatorHash.Store(&hh)
			utils.Debugf("[BOARDS] creatorHash from 431: %s", shortStr(hh, 8))
		}
	}
	sess.joined.Store(true)
	utils.Debugf("[BOARDS] join snapshot: %d bytes, %d objects, %d of them packets left on the board",
		snap.size, snap.objects, len(snap.stale))
	if len(snap.stale) > 0 {
		if len(snap.stale) >= 1000 {
			utils.Infof("[BOARDS] the board held %d packets nobody dropped (a join snapshot of %.1f MB): clearing them",
				len(snap.stale), float64(snap.size)/(1<<20))
		}
		sess.queueDrop(snap.stale...)
	}
}

func (t *BoardsTransport) sendSubscribe(sess *boardsSession, participant string) error {
	data := map[string]interface{}{
		"session":      sess.Info.session,
		"dashboard":    sess.Info.dashboard,
		"presentation": sess.Info.hash,
		"properties": map[string]interface{}{
			"guest_mode":                     true,
			"guest_role":                     1,
			"guest_password":                 nil,
			"guest_password_expiration_date": nil,
			"current_slide":                  sess.Info.currentSlide,
		},
		"participant_team_role": -1,
		"participant":           participant,
		"options": map[string]interface{}{
			"type": "landing",
			"participant": map[string]interface{}{
				"hash":         participant,
				"partner":      "yandex",
				"userHash":     sess.Info.userHash,
				"name":         sess.Info.name,
				"additional":   map[string]interface{}{"guest": true},
				"module":       "yandex",
				"presentation": sess.Info.hash,
				"identityCandidates": map[string]interface{}{
					"uidHash":    nil,
					"legacyHash": participant,
					"uid":        nil,
					"partner":    "yandex",
				},
				"module_type":            "yandex",
				"participantCaptionName": sess.Info.name,
			},
			"intermediate": "",
			"device": map[string]interface{}{
				"screen":              "674 x 619",
				"screen_width":        674,
				"screen_height":       619,
				"browser":             "Chrome",
				"browserVersion":      "153.0.0.0",
				"browserMajorVersion": 153,
				"mobile":              true,
				"os":                  "Android",
				"osVersion":           "15",
				"osMajorVersion":      15,
				"cookies":             true,
				"flashVersion":        "no check",
				"agent":               "Chrome",
				"appVersion":          boardsUA,
				"userAgent":           boardsUA,
				"appName":             "Netscape",
				"platform":            "MacIntel",
			},
		},
	}
	return sess.writeEventObj("dashboard", map[string]interface{}{
		"action":      "subscribe-slide-dashboard",
		"data":        data,
		"participant": participant,
	})
}

// ---- отправка ----
//
// Канал — modify-objects. Создаём текстовый объект с payload в value.
// Сервер broadcast'ит как server-modify-objects, который обрабатывается
// в handleServerModifyObjects.

func (t *BoardsTransport) writerLoop(sess *boardsSession) {
	queue := sess.Queue
	for {
		select {
		case <-t.done:
			return
		case <-sess.closed:
			return
		case pkt := <-queue:
			if err := t.sendNotifyPosition(sess, pkt); err != nil {
				utils.Debugf("[BOARDS] notify-position: %v", err)
			} else {
				t.RecordSend(len(pkt))
			}
		}
	}
}

// sendNotifyPosition отправляет modify-objects с текстовым объектом.
// Payload передаём в _attributes_.value как base64. Сервер broadcast'ит
// это как server-modify-objects, который peer'ы обрабатывают.
func (t *BoardsTransport) sendNotifyPosition(sess *boardsSession, pkt []byte) error {
	// Уникальный ID объекта и случайные координаты.
	id := randomHex(32)
	obj := buildModifyObjects(base64.StdEncoding.EncodeToString(pkt), id,
		mrand.Intn(2000), mrand.Intn(1200), *sess.creatorHash.Load(), *sess.participant.Load())
	if err := sess.writeEventObj("dashboard", obj); err != nil {
		return err
	}
	t.trackOwn(id, time.Now())
	return nil
}

// awaitPace waits until the socket's budget allows a packet, leaving the
// drops their reserve; false if the socket or the carrier closed meanwhile.
func (t *BoardsTransport) awaitPace(sess *boardsSession) bool {
	for {
		wait := sess.pace.take(time.Now(), boardsPaceReserve)
		if wait == 0 {
			return true
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-sess.closed:
			timer.Stop()
			return false
		case <-t.done:
			timer.Stop()
			return false
		}
	}
}

// dropLoop clears the board for this socket: the objects queued to drop
// (received packets, a join snapshot's leftovers) and our own objects the
// peer has left lying past boardsOwnTTL, in drop-objects events of up to
// boardsDropBatch hashes, boardsDropBurst of them per tick.
func (t *BoardsTransport) dropLoop(sess *boardsSession) {
	tick := time.NewTicker(boardsDropTick)
	defer tick.Stop()
	lastOwn := time.Now()
	for {
		select {
		case <-t.done:
			return
		case <-sess.closed:
			return
		case <-tick.C:
		}
		if now := time.Now(); now.Sub(lastOwn) >= time.Second {
			lastOwn = now
			if left := t.expiredOwn(now.Add(-t.ownTTL)); len(left) > 0 {
				utils.Debugf("[BOARDS] dropping %d own objects nobody dropped in %v", len(left), t.ownTTL)
				sess.queueDrop(left...)
			}
		}
		for i := 0; i < boardsDropBurst && sess.pendingDrops() > 0; i++ {
			if sess.pace.take(time.Now(), 0) > 0 {
				break // the budget is spent; the next tick
			}
			hashes := sess.takeDrops(boardsDropBatch)
			if err := sess.writeEventObj("dashboard", buildDropObjects(hashes, *sess.participant.Load())); err != nil {
				utils.Debugf("[BOARDS] drop-objects: %v", err)
				return
			}
			utils.Debugf("[BOARDS] dropped %d objects", len(hashes))
		}
	}
}

// buildDropObjects is the drop-objects event for the given objects. The
// server finds an object by its hash alone.
func buildDropObjects(hashes []string, participant string) map[string]interface{} {
	objs := make([]map[string]string, len(hashes))
	for i, h := range hashes {
		objs[i] = map[string]string{"hash": h}
	}
	return map[string]interface{}{
		"action":      "drop-objects",
		"data":        map[string]interface{}{"objects": objs},
		"participant": participant,
	}
}

// buildModifyObjects is the modify-objects event carrying one packet as a
// text object whose value is the base64 payload. Pure, and exported through
// BoardsModifyObjects so the JS port can be compared with it byte for byte.
func buildModifyObjects(b64, objID string, x, y int, creator, participant string) map[string]interface{} {
	return map[string]interface{}{
		"action": "modify-objects",
		"data": map[string]interface{}{
			"objects": []map[string]interface{}{
				{
					"_attributes_": map[string]interface{}{
						"id":          objID,
						"value":       b64, // payload как base64
						"style":       "text;html=1;strokeColor=none;fillColor=none;align=left;verticalAlign=middle;whiteSpace=wrap;rounded=0;fontSize=1;",
						"vertex":      "1",
						"type":        "textbox",
						"creatorHash": creator,
						"parent":      "DASHBOARD",
						"index":       "1",
					},
					"mxGeometry": []map[string]interface{}{
						{
							"_attributes_": map[string]interface{}{
								"x":      fmt.Sprintf("%d", x),
								"y":      fmt.Sprintf("%d", y),
								"width":  "1",
								"height": "1",
								"as":     "geometry",
							},
						},
					},
					"hash": objID,
				},
			},
			"valueChanges": map[string]bool{
				objID: true,
			},
		},
		"participant": participant,
	}
}

// BoardsModifyObjects is the JSON of the dashboard event buildModifyObjects
// makes: ["dashboard", {...}], as it goes on the wire after the "42<ack>"
// prefix.
func BoardsModifyObjects(b64, objID string, x, y int, creator, participant string) []byte {
	body, _ := json.Marshal([]interface{}{"dashboard", buildModifyObjects(b64, objID, x, y, creator, participant)})
	return body
}

// ---- keepalive ----

func (t *BoardsTransport) keepAliveLoop(sess *boardsSession) {
	tick := time.NewTicker(boardsPingInterval)
	defer tick.Stop()
	for {
		select {
		case <-sess.closed:
			return
		case <-t.done:
			return
		case <-tick.C:
			obj := map[string]interface{}{
				"action":      "heartbeat",
				"data":        map[string]interface{}{},
				"participant": *sess.participant.Load(),
			}
			if err := sess.writeEventObj("dashboard", obj); err != nil {
				utils.Debugf("[BOARDS] heartbeat: %v", err)
				return
			}
		}
	}
}

// ---- приём ----

func (t *BoardsTransport) handleMessage(sess *boardsSession, raw []byte) {
	if len(raw) == 1 && raw[0] == '2' {
		utils.Debugf("[BOARDS] ping -> pong")
		_ = sess.writeRaw("3")
		return
	}
	if len(raw) == 1 && raw[0] == '3' {
		return
	}

	if bytes.HasPrefix(raw, []byte("431[")) {
		// A snapshot handed over whole; readFrame parses a streamed one.
		snap, err := parseBoardsSnapshot(bytes.NewReader(raw[3:]))
		if err != nil {
			utils.Debugf("[BOARDS] join snapshot: %v", err)
		}
		snap.size = int64(len(raw))
		t.handleSnapshot(sess, snap)
		return
	}
	if bytes.HasPrefix(raw, []byte("43")) {
		t.handleAck(raw)
		return
	}
	if bytes.HasPrefix(raw, []byte("41")) || bytes.HasPrefix(raw, []byte("44")) {
		// socket.io DISCONNECT / CONNECT_ERROR: the server let go of us and
		// will deliver nothing more on this socket; reconnect.
		utils.Infof("[BOARDS] the board ended the subscription: %s", shortStr(string(raw), 200))
		_ = sess.Conn.Close()
		return
	}
	if !bytes.HasPrefix(raw, []byte("42[")) {
		return
	}
	idx := bytes.IndexByte(raw, '[')
	if idx < 0 {
		return
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(raw[idx:], &arr); err != nil {
		return
	}
	if len(arr) < 2 {
		return
	}

	var envelope struct {
		Action      string          `json:"action"`
		Data        json.RawMessage `json:"data"`
		Participant string          `json:"participant"`
	}
	if err := json.Unmarshal(arr[1], &envelope); err != nil {
		return
	}

	switch envelope.Action {
	case "participant-connected":
		t.handleParticipantConnected(sess, envelope.Data)
	case "server-modify-objects", "modify-objects":
		t.handleServerModifyObjects(sess, envelope.Data, envelope.Action)
	case "server-drop-objects", "drop-objects":
		// Someone (the peer, or we) dropped objects: ours among them need
		// no dropping by us.
		t.forgetOwn(droppedHashes(envelope.Data))
	case "participant-disconnected", "participant-rights-snapshot-supported", "in", "heartbeat":
	default:
		if utils.Throttled("boards.event."+envelope.Action, time.Hour) {
			utils.Debugf("[BOARDS] board event %q not handled: %s", envelope.Action, shortStr(string(arr[1]), 300))
		}
	}
}

// droppedHashes lists the objects a (server-)drop-objects event removed.
func droppedHashes(raw json.RawMessage) []string {
	var d struct {
		Objects []struct {
			Hash string `json:"hash"`
		} `json:"objects"`
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil
	}
	out := make([]string, 0, len(d.Objects))
	for _, o := range d.Objects {
		if o.Hash != "" {
			out = append(out, o.Hash)
		}
	}
	return out
}

// handleAck reads the server's answer to one of our events. It is
// [{"result":true}] or, say, [{"error":{"code":200,"securityCode":
// "PAYLOAD_TOO_LARGE"}}] for a message over the server's limit - refusals
// that used to vanish here, so the logs could not show them.
func (t *BoardsTransport) handleAck(raw []byte) {
	if !bytes.Contains(raw, []byte(`"error"`)) {
		return
	}
	if utils.Throttled("boards.refused", time.Minute) {
		utils.Infof("[BOARDS] the board refused a message: %s", shortStr(string(raw), 200))
	}
}

func (t *BoardsTransport) handleParticipantConnected(sess *boardsSession, raw json.RawMessage) {
	var d struct {
		Participant struct {
			Hash    string `json:"hash"`
			Session string `json:"session"`
			Name    string `json:"name"`
		} `json:"participant"`
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		return
	}
	utils.Debugf("[BOARDS] participant-connected: name=%q hash=%s session=%s",
		d.Participant.Name, shortStr(d.Participant.Hash, 8), shortStr(d.Participant.Session, 8))

	if d.Participant.Session != "" && sess.Info.session == "" {
		sess.Info.session = d.Participant.Session
	}

	if d.Participant.Name == sess.Info.name && d.Participant.Hash != "" {
		h := d.Participant.Hash
		sess.creatorHash.Store(&h)
		utils.Debugf("[BOARDS] creatorHash from participant-connected: %s", shortStr(h, 8))
		// Our own arrival: the server has us on the dashboard.
		sess.joined.Store(true)
	}
}

func (t *BoardsTransport) handleServerModifyObjects(sess *boardsSession, raw json.RawMessage, action string) {
	payloads, toDelete := ModifyObjectsPayloads(raw, *sess.participant.Load(), sess.Info.userHash, sess.Info.name)

	for _, decoded := range payloads {
		utils.Debugf("[BOARDS<-] %s pktlen=%d", action, len(decoded))
		t.RecordReceive(len(decoded))
		t.onDataMu.RLock()
		cb := t.onData
		t.onDataMu.RUnlock()
		if cb != nil {
			cb(decoded)
		}
	}

	// Received objects leave the board with the next drop batch, by hash.
	for _, o := range toDelete {
		if h, _ := o["hash"].(string); h != "" {
			sess.queueDrop(h)
		}
	}
}

// ModifyObjectsPayloads reads a (server-)modify-objects event: the packets
// carried by objects that are not our own echo (decoded, in order), and the
// drop-objects entries that delete exactly those objects from the board.
// Pure, and exported so the JS port can be compared with it.
func ModifyObjectsPayloads(raw json.RawMessage, myPart, myUser, myName string) (payloads [][]byte, toDelete []map[string]interface{}) {
	var d struct {
		Dashboard string `json:"dashboard"`
		Name      string `json:"name"`
		Session   string `json:"session"`
		Objects   []struct {
			Attributes map[string]interface{}   `json:"_attributes_"`
			Hash       string                   `json:"hash"`
			Geometry   []map[string]interface{} `json:"mxGeometry"`
		} `json:"objects"`
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, nil
	}
	for _, o := range d.Objects {
		val, _ := o.Attributes["value"].(string)
		if val == "" {
			continue
		}
		creator, _ := o.Attributes["creatorHash"].(string)

		if creator != "" && (creator == myPart || creator == myUser) {
			continue // own echo
		}
		if d.Name != "" && d.Name == myName {
			continue // own echo
		}

		decoded, err := base64.StdEncoding.DecodeString(val)
		if err != nil || len(decoded) == 0 {
			continue
		}
		payloads = append(payloads, decoded)

		// Собираем объект для удаления
		toDelete = append(toDelete, map[string]interface{}{
			"_attributes_": o.Attributes,
			"mxGeometry":   o.Geometry,
			"hash":         o.Hash,
		})
	}
	return payloads, toDelete
}

// jar returns the transport's shared cookie jar (never nil).
func (t *BoardsTransport) jar() *cookiejar.Jar {
	t.jarMu.RLock()
	jar := t.cookieJar
	t.jarMu.RUnlock()
	if jar == nil {
		jar, _ = cookiejar.New(nil)
		t.jarMu.Lock()
		t.cookieJar = jar
		t.jarMu.Unlock()
	}
	return jar
}

// ---- CookieExchanger ----

// FetchCookies returns a snapshot of the transport's current cookie jar as
// name -> value for boards.yandex.ru.
func (t *BoardsTransport) FetchCookies() (map[string]string, error) {
	u, err := url.Parse("https://" + boardsBase + "/")
	if err != nil {
		return nil, err
	}
	out := make(map[string]string)
	for _, c := range t.jar().Cookies(u) {
		out[c.Name] = c.Value
	}
	return out, nil
}

// ApplyCookies replaces the transport's cookie jar and forces the current WS
// session to reconnect.
func (t *BoardsTransport) ApplyCookies(values map[string]string) error {
	if len(values) == 0 {
		return nil
	}
	u, _ := url.Parse("https://" + boardsBase + "/")
	jar, _ := cookiejar.New(nil)
	cookies := make([]*http.Cookie, 0, len(values))
	for k, v := range values {
		cookies = append(cookies, &http.Cookie{Name: k, Value: v, Path: "/"})
	}
	jar.SetCookies(u, cookies)

	t.jarMu.Lock()
	t.cookieJar = jar
	t.jarMu.Unlock()

	utils.Debugf("[BOARDS] applied %d cookies, forcing reconnect", len(cookies))

	if s := t.session.Load(); s != nil && s.Conn != nil {
		_ = s.Conn.Close()
	}
	return nil
}

// network is the network this carrier dials through (TransportConfig.Network).
func (t *BoardsTransport) network() string {
	if t.BaseTransport == nil {
		return ""
	}
	return t.GetConfig().Network
}
