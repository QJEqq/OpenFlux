package yandex

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const testBoardHash = "0123456789abcdef0123456789abcdef"

// fakeBoards is a boards.yandex.ru double: the owner API (init-csrf, make,
// start, update-dashboard-presentation with a rotating csrf) and the guest
// join, which hands out token_<hash> only once the board is open — after the
// action the test picks, and lag guest checks later (guest mode lands with a
// delay, as it does live).
type fakeBoards struct {
	mu      sync.Mutex
	openOn  string // "start-whiteboard", "update-dashboard-presentation" or "" (never)
	lag     int    // guest checks still refused after the board opens
	open    bool
	guests  int // request-guest-token calls
	calls   []string
	csrfIn  map[string]string // csrf each owner action carried
	updated map[string]interface{}
	counter int
}

func (f *fakeBoards) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Method == "GET" {
		w.WriteHeader(http.StatusOK)
		return
	}
	var body struct{ Action, Content string }
	_ = json.NewDecoder(r.Body).Decode(&body)
	raw, _ := base64.StdEncoding.DecodeString(body.Content)
	var content map[string]interface{}
	_ = json.Unmarshal(raw, &content)
	f.calls = append(f.calls, body.Action)
	if csrf, ok := content["csrf"].(string); ok {
		f.csrfIn[body.Action] = csrf
	}
	f.counter++
	next := map[string]interface{}{"csrf": "c" + string(rune('0'+f.counter))}
	switch body.Action {
	case "make-whiteboard":
		next["hash"] = testBoardHash
	case "start-whiteboard", "update-dashboard-presentation":
		if body.Action == "update-dashboard-presentation" {
			f.updated = content
		}
		if f.openOn == body.Action {
			f.open = true
		}
		next["result"] = true
	case "request-guest-token":
		f.guests++
		if f.open && f.lag == 0 {
			http.SetCookie(w, &http.Cookie{Name: "token_" + testBoardHash, Value: "jwt", Path: "/"})
			next = map[string]interface{}{"redirect": "https://boards.yandex.ru/whiteboard/?hash=" + testBoardHash}
		} else {
			if f.open {
				f.lag--
			}
			next = map[string]interface{}{"redirect": "https://boards.yandex.ru"}
		}
	}
	_ = json.NewEncoder(w).Encode(next)
}

func runCreateBoard(t *testing.T, openOn string, lag int) (*fakeBoards, string, error) {
	t.Helper()
	wait := boardsGuestWait
	boardsGuestWait = time.Millisecond
	t.Cleanup(func() { boardsGuestWait = wait })
	f := &fakeBoards{openOn: openOn, lag: lag, csrfIn: map[string]string{}}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	jar, _ := cookiejar.New(nil)
	owner := &http.Client{Jar: jar, Transport: toServer{srv}}
	guest := &http.Client{Transport: toServer{srv}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	u, err := createBoard(context.Background(), owner, guest, "")
	return f, u, err
}

func TestCreateBoardStartsTheBoardForGuests(t *testing.T) {
	f, u, err := runCreateBoard(t, "start-whiteboard", 0)
	if err != nil || u != "https://boards.yandex.ru/whiteboard/?hash="+testBoardHash {
		t.Fatalf("url %q err %v", u, err)
	}
	// make-whiteboard's answer rotated the csrf; start-whiteboard must carry it.
	if f.csrfIn["start-whiteboard"] != "c2" {
		t.Fatalf("start-whiteboard csrf %q, want the rotated c2 (calls %v)", f.csrfIn["start-whiteboard"], f.calls)
	}
}

func TestCreateBoardTurnsGuestModeOn(t *testing.T) {
	f, u, err := runCreateBoard(t, "update-dashboard-presentation", 0)
	if err != nil || !strings.HasSuffix(u, testBoardHash) {
		t.Fatalf("url %q err %v", u, err)
	}
	props, _ := f.updated["properties"].(map[string]interface{})
	if f.updated["media"] != testBoardHash || props["guest_mode"] != true || props["guest_role"] != float64(1) {
		t.Fatalf("update-dashboard-presentation content %v", f.updated)
	}
	// start-whiteboard's answer rotated the csrf again.
	if f.csrfIn["update-dashboard-presentation"] != "c3" {
		t.Fatalf("update csrf %q, want c3 (calls %v)", f.csrfIn["update-dashboard-presentation"], f.calls)
	}
}

// Guest mode lands a few seconds after the update: the first checks are
// refused, and createBoard must keep checking instead of dropping the board.
func TestCreateBoardWaitsForGuestModeToLand(t *testing.T) {
	f, u, err := runCreateBoard(t, "update-dashboard-presentation", 3)
	if err != nil || !strings.HasSuffix(u, testBoardHash) {
		t.Fatalf("url %q err %v", u, err)
	}
	if f.guests != 4 {
		t.Fatalf("guest checks %d, want 4 (3 refused, then in)", f.guests)
	}
}

func TestCreateBoardRefusesABoardGuestsCannotJoin(t *testing.T) {
	f, u, err := runCreateBoard(t, "", 0)
	if err == nil || u != "" || !strings.Contains(err.Error(), "гостей не пускает") || !strings.Contains(err.Error(), `"redirect"`) {
		t.Fatalf("url %q err %v", u, err)
	}
	if f.guests != boardsGuestChecks {
		t.Fatalf("guest checks %d, want %d", f.guests, boardsGuestChecks)
	}
}
