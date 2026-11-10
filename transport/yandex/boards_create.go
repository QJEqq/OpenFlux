package yandex

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"time"
)

// CreateBoard makes a new Yandex board on the operator's account and returns
// its share URL (https://boards.yandex.ru/whiteboard/?hash=<32hex>), ready for
// the boards transport. It is the programmatic form of the cabinet's "Новая
// доска" button: the augmenter calls it (over --node-wizard) with a pooled
// operator's cookies so one multi-transport channel per user can be built
// without a human opening boards.yandex.ru.
//
// cookies are the operator's Yandex session cookies (Session_id, sessionid2,
// L, ...), domain-wide for .yandex.ru. The exit and the client both join the
// board as guests (see BoardsTransport.authorize → request-guest-token).
//
// CSRF is bootstrapped at runtime, exactly as the cabinet SPA does it
// (lib.cabinet.js wbpost): POST /api init-csrf needs no prior token and
// returns a fresh csrf JWT; every owner call carries the latest one, and a
// csrf in an answer replaces it.
//
// A board made with make-whiteboard alone is not open to guests: live, its
// request-guest-token answered {"redirect":"https://boards.yandex.ru"} with no
// token_<hash>, so the boards carrier could never join, while a board its
// owner had opened in the browser worked — its presentation carries
// guest_mode:true, guest_role:1. Neither starting the board nor its link role
// (access_type) opens it; switching guest mode on (update-dashboard-presentation)
// does, live, a few seconds later. So the board is started the way the cabinet
// does when its owner opens it (start-whiteboard), guest mode is switched on,
// and the board is checked as an anonymous guest until it lets one in
// (boardsGuestWait). A board guests cannot join is an error, not a dead carrier.
func CreateBoard(ctx context.Context, cookies map[string]string, name string) (string, error) {
	if len(cookies) == 0 {
		return "", fmt.Errorf("нет cookies Яндекса")
	}
	jar, _ := cookiejar.New(nil)
	u, _ := url.Parse("https://" + boardsBase + "/")
	cs := make([]*http.Cookie, 0, len(cookies))
	for k, v := range cookies {
		cs = append(cs, &http.Cookie{Name: k, Value: v, Path: "/"})
	}
	jar.SetCookies(u, cs)
	owner := &http.Client{Jar: jar, Transport: carrierTransport(""), Timeout: 20 * time.Second}
	guest := &http.Client{
		Transport: carrierTransport(""),
		Timeout:   20 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return createBoard(ctx, owner, guest, name)
}

// createBoard is CreateBoard with its two clients: owner carries the operator's
// sign-in, guest none (each check starts from an empty jar).
func createBoard(ctx context.Context, owner, guest *http.Client, name string) (string, error) {
	if name == "" {
		name = "Доска"
	}
	csrf, err := boardsInitCSRF(ctx, owner)
	if err != nil {
		return "", fmt.Errorf("init-csrf: %w", err)
	}
	hash, csrf, err := boardsMakeWhiteboard(ctx, owner, name, csrf)
	if err != nil {
		return "", fmt.Errorf("make-whiteboard: %w", err)
	}
	if !isBoardHash(hash) {
		return "", fmt.Errorf("make-whiteboard вернул неожиданный hash %q", shortStr(hash, 40))
	}
	boardURL := "https://" + boardsBase + "/whiteboard/?hash=" + hash

	if csrf, err = boardsOwnerCall(ctx, owner, "start-whiteboard", map[string]interface{}{"hash": hash}, csrf); err != nil {
		return "", fmt.Errorf("start-whiteboard: %w", err)
	}
	// Guest mode is a property of the presentation: the boards guests can join
	// carry guest_mode:true, guest_role:1 (seen in get-whiteboard-info). The
	// editor writes properties with update-dashboard-presentation; the cabinet
	// only has the switch-off (disable-presentation-guest-mode).
	if _, err = boardsOwnerCall(ctx, owner, "update-dashboard-presentation", map[string]interface{}{
		"media": hash,
		"properties": map[string]interface{}{
			"guest_mode": true, "guest_role": 1, "guest_password": nil, "guest_password_expiration_date": nil,
		},
	}, csrf); err != nil {
		return "", fmt.Errorf("update-dashboard-presentation: %w", err)
	}
	// Guest mode lands a few seconds after the update (live: an immediate check
	// was refused, the same board let a guest in moments later), so the check
	// polls instead of judging the first answer.
	var why string
	for i := 0; i < boardsGuestChecks; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				return "", fmt.Errorf("доска %s: гостевая проверка прервана: %w", hash, ctx.Err())
			case <-time.After(boardsGuestWait):
			}
		}
		var open bool
		if open, why = boardsGuestCanJoin(ctx, guest, hash); open {
			return boardURL, nil
		}
	}
	return "", fmt.Errorf("доска %s создана, но гостей не пускает: %s", hash, why)
}

// How long createBoard waits for guest mode to take effect: boardsGuestChecks
// tries, boardsGuestWait apart (variables so tests can shorten the wait).
var (
	boardsGuestChecks = 8
	boardsGuestWait   = 2 * time.Second
)

// boardsOwnerCall makes one owner (cabinet) call the way wbpost does: the
// current csrf goes into the content, and a csrf in the answer replaces it.
// It returns the csrf to use next.
func boardsOwnerCall(ctx context.Context, client *http.Client, action string, content map[string]interface{}, csrf string) (string, error) {
	content["csrf"] = csrf
	req, err := boardsCabinetRequest(ctx, action, content)
	if err != nil {
		return csrf, err
	}
	var r struct {
		CSRF  string          `json:"csrf"`
		Error *boardsAPIError `json:"error"`
	}
	if err := boardsDo(client, req, &r); err != nil {
		return csrf, err
	}
	if r.Error != nil {
		return csrf, r.Error
	}
	if r.CSRF != "" {
		csrf = r.CSRF
	}
	return csrf, nil
}

// boardsGuestCanJoin checks the board the way the boards carrier joins it, as
// an anonymous guest from an empty jar: GET the board, then request-guest-token
// must set token_<hash>. A captcha or a network error cannot tell either way
// and counts as open (the carrier solves the PoW captcha itself); a refusal
// comes back with the server's answer.
func boardsGuestCanJoin(ctx context.Context, guest *http.Client, hash string) (bool, string) {
	c := *guest
	c.Jar, _ = cookiejar.New(nil)
	req, _ := http.NewRequestWithContext(ctx, "GET", "https://"+boardsBase+"/whiteboard/?hash="+hash, nil)
	req.Header.Set("User-Agent", boardsUA)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	resp, err := c.Do(req)
	if err != nil {
		return true, "не проверить: " + err.Error()
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	if loc := resp.Header.Get("Location"); strings.Contains(loc, "captcha") {
		return true, "не проверить: капча"
	}
	tokenReq := apiRequest(hash, "request-guest-token", map[string]string{"name": "guest_" + randomHex(3), "hash": hash}).WithContext(ctx)
	resp, err = c.Do(tokenReq)
	if err != nil {
		return true, "не проверить: " + err.Error()
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<12))
	resp.Body.Close()
	u, _ := url.Parse("https://" + boardsBase + "/")
	for _, ck := range c.Jar.Cookies(u) {
		if ck.Name == "token_"+hash {
			return true, ""
		}
	}
	return false, "request-guest-token → " + shortStr(strings.TrimSpace(string(body)), 120)
}

// boardsCabinetRequest builds an authenticated POST /api call from the cabinet
// (Referer /cabinet/, not /guest/): action + base64(JSON content), same body
// shape as apiRequest but for the owner flow.
func boardsCabinetRequest(ctx context.Context, action string, content interface{}) (*http.Request, error) {
	raw, _ := json.Marshal(content)
	body, _ := json.Marshal(map[string]string{
		"action":  action,
		"content": base64.StdEncoding.EncodeToString(raw),
	})
	req, err := http.NewRequestWithContext(ctx, "POST", "https://"+boardsBase+"/api", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", boardsUA)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Accept", "application/json, text/javascript, */*; q=0.01")
	req.Header.Set("Origin", "https://"+boardsBase)
	req.Header.Set("Referer", "https://"+boardsBase+"/cabinet/")
	return req, nil
}

// boardsAPIError is the {"error":{"code","description"}} the API returns, e.g.
// {"code":104,"description":"WRONG_CSRF"} when a call is sent without a csrf.
type boardsAPIError struct {
	Code        int    `json:"code"`
	Description string `json:"description"`
}

func (e *boardsAPIError) Error() string {
	if e.Description != "" {
		return fmt.Sprintf("%s (код %d)", e.Description, e.Code)
	}
	return fmt.Sprintf("код %d", e.Code)
}

func boardsDo(client *http.Client, req *http.Request, out interface{}) error {
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode != 200 {
		return fmt.Errorf("status %d: %s", resp.StatusCode, shortStr(string(body), 200))
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("ответ не JSON: %s", shortStr(string(body), 120))
	}
	return nil
}

// boardsInitCSRF bootstraps a csrf token: init-csrf needs no prior token and
// returns one in {"csrf": ...}. An empty csrf means the cookies are not a live
// signed-in Yandex session.
func boardsInitCSRF(ctx context.Context, client *http.Client) (string, error) {
	req, err := boardsCabinetRequest(ctx, "init-csrf", map[string]interface{}{})
	if err != nil {
		return "", err
	}
	var r struct {
		CSRF  string          `json:"csrf"`
		Error *boardsAPIError `json:"error"`
	}
	if err := boardsDo(client, req, &r); err != nil {
		return "", err
	}
	if r.Error != nil {
		return "", r.Error
	}
	if r.CSRF == "" {
		return "", fmt.Errorf("пустой csrf — похоже, cookies не авторизованы")
	}
	return r.CSRF, nil
}

// boardsMakeWhiteboard creates the board and returns its hash and the csrf to
// use next. access_type 10 is what the cabinet's "Новая доска" sends.
func boardsMakeWhiteboard(ctx context.Context, client *http.Client, name, csrf string) (string, string, error) {
	req, err := boardsCabinetRequest(ctx, "make-whiteboard", map[string]interface{}{
		"name":        name,
		"access_type": 10,
		"csrf":        csrf,
	})
	if err != nil {
		return "", csrf, err
	}
	var r struct {
		Hash  string          `json:"hash"`
		CSRF  string          `json:"csrf"`
		Error *boardsAPIError `json:"error"`
	}
	if err := boardsDo(client, req, &r); err != nil {
		return "", csrf, err
	}
	if r.Error != nil {
		return "", csrf, r.Error
	}
	if r.CSRF != "" {
		csrf = r.CSRF
	}
	return r.Hash, csrf, nil
}

// isBoardHash reports whether h is a 32-char lowercase-hex board hash, the
// form node-install.sh validates in a boards URL.
func isBoardHash(h string) bool {
	if len(h) != 32 {
		return false
	}
	for _, c := range h {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}
