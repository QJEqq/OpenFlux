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
// L, ...), domain-wide for .yandex.ru. access_type 10 is the cabinet's default
// for a new board; the exit and the client both join it as guests (see
// BoardsTransport.authorize → request-guest-token), which that level allows.
//
// CSRF is bootstrapped at runtime, exactly as the cabinet SPA does it
// (lib.cabinet.js wbpost): POST /api init-csrf needs no prior token and
// returns a fresh csrf JWT, which make-whiteboard then requires. The token
// rotates on every response, so a single create needs only this one.
func CreateBoard(ctx context.Context, cookies map[string]string, name string) (string, error) {
	if len(cookies) == 0 {
		return "", fmt.Errorf("нет cookies Яндекса")
	}
	if name == "" {
		name = "Доска"
	}

	jar, _ := cookiejar.New(nil)
	u, _ := url.Parse("https://" + boardsBase + "/")
	cs := make([]*http.Cookie, 0, len(cookies))
	for k, v := range cookies {
		cs = append(cs, &http.Cookie{Name: k, Value: v, Path: "/"})
	}
	jar.SetCookies(u, cs)

	client := &http.Client{
		Jar:       jar,
		Transport: carrierTransport(""),
		Timeout:   20 * time.Second,
	}

	csrf, err := boardsInitCSRF(ctx, client)
	if err != nil {
		return "", fmt.Errorf("init-csrf: %w", err)
	}
	hash, err := boardsMakeWhiteboard(ctx, client, name, csrf)
	if err != nil {
		return "", fmt.Errorf("make-whiteboard: %w", err)
	}
	if !isBoardHash(hash) {
		return "", fmt.Errorf("make-whiteboard вернул неожиданный hash %q", shortStr(hash, 40))
	}
	return "https://" + boardsBase + "/whiteboard/?hash=" + hash, nil
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

// boardsMakeWhiteboard creates the board and returns its hash.
func boardsMakeWhiteboard(ctx context.Context, client *http.Client, name, csrf string) (string, error) {
	req, err := boardsCabinetRequest(ctx, "make-whiteboard", map[string]interface{}{
		"name":        name,
		"access_type": 10,
		"csrf":        csrf,
	})
	if err != nil {
		return "", err
	}
	var r struct {
		Hash  string          `json:"hash"`
		Error *boardsAPIError `json:"error"`
	}
	if err := boardsDo(client, req, &r); err != nil {
		return "", err
	}
	if r.Error != nil {
		return "", r.Error
	}
	return r.Hash, nil
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
