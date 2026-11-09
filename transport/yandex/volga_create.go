package yandex

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// CreateVolgaDoc makes a new editable Yandex document on the operator's Disk
// and returns its editor URL (https://docs.yandex.ru/edit/d/<id>?source=docs),
// shared "edit by link" — ready for the vyandex (volga) transport. It is the
// programmatic form of docs.yandex.ru "Создать → Документ" + the share dialog,
// so the augmenter can build a per-user volga lane from a pooled account's
// cookies.
//
// docs.yandex.ru is a front over Yandex Disk (MPFS). A volga doc is a .docx on
// Disk edited over WOPI. The flow, reverse-engineered from the web client:
//
//  1. tokens — the authed docs.yandex.ru/docs page carries two: sk
//     ("<hex>:<ts>", in the body of every MPFS call) and skExternal ("y<hex>",
//     guards the create route).
//  2. create — GET disk.yandex.ru/editnew/docx/disk?sk=<skExternal>&source=docs
//     &filename=<name>: what the client's "Создать → Документ" opens (action
//     editor.open). It creates /disk/<name>.docx and redirects to the editor.
//     Opening the editor with mpfs/office_action_data on a new path does NOT
//     create the file — MPFS answers 404 "resource not found" (seen live).
//  3. info — POST mpfs/info{path} → the file's resource_id ("<uid>:<64hex>"),
//     polled briefly in case the file lands a moment after the redirect.
//  4. set-public{path} → public link on; its response carries the /edit/d/<id>
//     sharing URL (office_access_state starts "disabled").
//  5. office-set-access-state{resourceId, accessState:"all"} → edit-by-link.
func CreateVolgaDoc(ctx context.Context, cookies map[string]string) (string, error) {
	if len(cookies) == 0 {
		return "", fmt.Errorf("нет cookies Яндекса")
	}

	// Domain cookies: the sign-in is .yandex.ru-wide, and the flow spans
	// docs.yandex.ru (the API) and disk.yandex.ru (the create route).
	jar, _ := cookiejar.New(nil)
	u, _ := url.Parse("https://docs.yandex.ru/")
	cs := make([]*http.Cookie, 0, len(cookies))
	for k, v := range cookies {
		cs = append(cs, &http.Cookie{Name: k, Value: v, Path: "/", Domain: "yandex.ru"})
	}
	jar.SetCookies(u, cs)
	client := &http.Client{Jar: jar, Transport: carrierTransport(""), Timeout: 25 * time.Second}

	sk, skExternal, err := volgaTokens(ctx, client)
	if err != nil {
		return "", err
	}

	name := "doc_" + randomHex(10)
	path := "/disk/" + name + ".docx"

	// 2) create the .docx (the editor redirect names the file it made).
	created, err := volgaCreate(ctx, client, skExternal, name)
	if err != nil {
		return "", fmt.Errorf("editnew: %w", err)
	}
	if created != "" {
		path = created
	}

	// 3) read the created file: resource_id (for sharing). office_online_sharing_url
	// may also appear here, but set-public's response is the reliable source.
	info, err := volgaInfo(ctx, client, sk, path)
	if err != nil {
		return "", err
	}
	resourceID := string(volgaResourceIDRe.FindSubmatch(info)[1])

	// 4) publish the file; its response carries the /edit/d/ sharing URL.
	pub, err := volgaAPI(ctx, client, sk, "edit/api", "mpfs/set-public", map[string]interface{}{
		"allowDefaultSettingsAvailable": true, "path": path, "type": "file",
	})
	if err != nil {
		return "", fmt.Errorf("set-public: %w", err)
	}
	share := ""
	if m := volgaSharingRe.FindSubmatch(pub); m != nil {
		share = string(m[1])
	} else if m := volgaSharingRe.FindSubmatch(info); m != nil {
		share = string(m[1])
	}
	if share == "" {
		return "", fmt.Errorf("не получили office_online_sharing_url")
	}

	// 5) switch access to edit-by-link (accessState "all").
	if _, err := volgaAPI(ctx, client, sk, "edit/api", "mpfs/office-set-access-state", map[string]interface{}{
		"resourceId": resourceID, "accessState": "all",
	}); err != nil {
		return "", fmt.Errorf("office-set-access-state: %w", err)
	}

	return share + "?source=docs", nil
}

// The page state is embedded as JSON, sometimes inside a JS string ("\"sk\":\"…\""),
// so the token patterns tolerate escaped quotes.
var (
	volgaSKRe         = regexp.MustCompile(`\\?"sk\\?"\s*:\s*\\?"([0-9a-f]{20,}(?::\d+)?)\\?"`)
	volgaSKExternalRe = regexp.MustCompile(`\\?"skExternal\\?"\s*:\s*\\?"([a-z]?[0-9a-f]{20,})\\?"`)
	volgaResourceIDRe = regexp.MustCompile(`"resource_id"\s*:\s*"(\d+:[0-9a-f]{64})"`)
	volgaSharingRe    = regexp.MustCompile(`"office_online_sharing_url"\s*:\s*"([^"]+)"`)
)

// volgaTokens fetches the authed docs.yandex.ru/docs page and reads the two Disk
// tokens: sk for the MPFS API and skExternal for the create route (the Disk
// client page carries it too, tried when the Docs one does not). An
// unauthenticated session redirects to passport and has neither.
func volgaTokens(ctx context.Context, client *http.Client) (sk, skExternal string, err error) {
	body, err := volgaPage(ctx, client, "https://docs.yandex.ru/docs")
	if err != nil {
		return "", "", fmt.Errorf("sk: %w", err)
	}
	m := volgaSKRe.FindSubmatch(body)
	if m == nil {
		return "", "", fmt.Errorf("sk: не найден (cookies не авторизованы?)")
	}
	sk = string(m[1])
	if m := volgaSKExternalRe.FindSubmatch(body); m != nil {
		return sk, string(m[1]), nil
	}
	if body, err := volgaPage(ctx, client, "https://disk.yandex.ru/client/disk"); err == nil {
		if m := volgaSKExternalRe.FindSubmatch(body); m != nil {
			return sk, string(m[1]), nil
		}
	}
	return "", "", fmt.Errorf("skExternal: не найден ни на docs.yandex.ru/docs, ни на disk.yandex.ru/client")
}

// volgaPage GETs an authed HTML page (its first 2 MiB; the tokens sit early).
func volgaPage(ctx context.Context, client *http.Client, pageURL string) ([]byte, error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", pageURL, nil)
	req.Header.Set("User-Agent", boardsUA)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(io.LimitReader(resp.Body, 1<<21))
}

// volgaCreate makes the new .docx the way Disk/Docs' "Создать → Документ" does
// (action editor.open): GET disk.yandex.ru/editnew/docx/<folder>?sk=<skExternal>
// &source=docs&filename=<name>. The route is guarded by skExternal — without it
// the client only asks "Создать новый документ?" and repeats the URL with it —
// creates <folder>/<name>.docx and redirects to the editor. It returns the
// created path when the final editor URL names it (/edit/disk/disk%2F<name>.docx),
// else "".
func volgaCreate(ctx context.Context, client *http.Client, skExternal, name string) (string, error) {
	q := url.Values{"sk": {skExternal}, "source": {"docs"}, "filename": {name}}
	req, _ := http.NewRequestWithContext(ctx, "GET", "https://disk.yandex.ru/editnew/docx/disk?"+q.Encode(), nil)
	req.Header.Set("User-Agent", boardsUA)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Referer", "https://docs.yandex.ru/docs")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<21))
	final := resp.Request.URL
	where := final.Host + final.Path // no query: it may carry tokens
	switch {
	case strings.Contains(final.Host, "passport"):
		return "", fmt.Errorf("вход не принят (редирект на %s)", where)
	case strings.Contains(final.Path, "captcha"):
		return "", fmt.Errorf("капча (%s)", where)
	case resp.StatusCode >= 400:
		return "", fmt.Errorf("status %d (%s)", resp.StatusCode, where)
	}
	const editor = "/edit/disk/"
	if p := final.EscapedPath(); strings.Contains(p, editor) {
		doc, err := url.PathUnescape(p[strings.Index(p, editor)+len(editor):])
		if err == nil && strings.HasSuffix(doc, ".docx") {
			return "/" + strings.TrimPrefix(doc, "/"), nil
		}
	}
	return "", nil
}

// volgaInfo reads the new file's info, polling briefly: the file may land a
// moment after the editor redirect.
func volgaInfo(ctx context.Context, client *http.Client, sk, path string) ([]byte, error) {
	var last error
	for i := 0; i < 8; i++ {
		info, err := volgaAPI(ctx, client, sk, "models-v2", "mpfs/info", map[string]interface{}{"path": path})
		if err == nil && volgaResourceIDRe.Match(info) {
			return info, nil
		}
		last = err
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(1500 * time.Millisecond):
		}
	}
	if last == nil {
		last = fmt.Errorf("нет resource_id")
	}
	return nil, fmt.Errorf("документ %s не появился после editnew: %w", path, last)
}

// volgaAPI makes one docs.yandex.ru MPFS call: POST /<ep>?m=<method> with
// {"apiMethod","requestParams","sk"} and returns the response body. ep is
// "edit/api" (editor/share ops) or "models-v2" (resource reads).
func volgaAPI(ctx context.Context, client *http.Client, sk, ep, method string, params map[string]interface{}) ([]byte, error) {
	reqBody, _ := json.Marshal(map[string]interface{}{"apiMethod": method, "requestParams": params, "sk": sk})
	req, err := http.NewRequestWithContext(ctx, "POST", "https://docs.yandex.ru/"+ep+"?m="+method, bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", boardsUA)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Origin", "https://docs.yandex.ru")
	req.Header.Set("Referer", "https://docs.yandex.ru/docs?type=docx")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, shortStr(string(body), 180))
	}
	// Surface an API-level {"error":...} as an error.
	var probe struct {
		Error json.RawMessage `json:"error"`
	}
	if json.Unmarshal(body, &probe) == nil && len(probe.Error) > 0 && string(probe.Error) != "null" {
		return nil, fmt.Errorf("api error: %s", shortStr(string(probe.Error), 180))
	}
	return body, nil
}
