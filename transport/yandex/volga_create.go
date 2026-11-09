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
	"time"
)

// CreateVolgaDoc makes a new editable Yandex document on the operator's Disk
// and returns its editor URL (https://docs.yandex.ru/edit/d/<id>?source=docs),
// shared "edit by link" — ready for the vyandex (volga) transport. It is the
// programmatic form of docs.yandex.ru "Создать → Текстовый документ" + the
// share dialog, so the augmenter can build a per-user volga lane from a pooled
// account's cookies.
//
// docs.yandex.ru is a front over Yandex Disk (MPFS). A volga doc is a .docx on
// Disk edited over WOPI. The flow, reverse-engineered from the web client:
//
//  1. sk   — Disk's CSRF token, read from the authed docs.yandex.ru/docs HTML
//            ("sk":"<hex>:<ts>"). Every API call carries it in the body.
//  2. create+open — POST mpfs/office_action_data for a NEW idDoc path. Opening
//            the office editor on a non-existent path makes the backend create
//            the stub .docx (the "office/locking_operation_stub_separated" op).
//  3. info — POST mpfs/info{path} → the file's resource_id ("<uid>:<64hex>")
//            and office_online_sharing_url (the /edit/d/<id> URL).
//  4. set-public{path}            → public link on (office_access_state starts
//            "disabled").
//  5. office-set-access-state{resourceId, accessState:"all"} → edit-by-link.
//
// NOTE: step 2 as the create trigger is the one part not captured end-to-end
// (the web client navigated to the editor, which cleared the netlog); it is
// strongly implied by the stub op and the absence of any other create call.
// If a node ever reports the doc missing after this, the editor-page GET may be
// needed before office_action_data. Validate on a node with a real signed-in
// account (exports redact sessionid2, so this cannot be exercised offline).
func CreateVolgaDoc(ctx context.Context, cookies map[string]string) (string, error) {
	if len(cookies) == 0 {
		return "", fmt.Errorf("нет cookies Яндекса")
	}

	jar, _ := cookiejar.New(nil)
	u, _ := url.Parse("https://docs.yandex.ru/")
	cs := make([]*http.Cookie, 0, len(cookies))
	for k, v := range cookies {
		cs = append(cs, &http.Cookie{Name: k, Value: v, Path: "/"})
	}
	jar.SetCookies(u, cs)
	client := &http.Client{Jar: jar, Transport: carrierTransport(""), Timeout: 25 * time.Second}

	sk, err := volgaSK(ctx, client)
	if err != nil {
		return "", fmt.Errorf("sk: %w", err)
	}

	name := "doc_" + randomHex(10)
	idDoc := "disk/" + name + ".docx"
	path := "/disk/" + name + ".docx"

	// 2) create + open the office editor on the new path.
	if _, err := volgaAPI(ctx, client, sk, "edit/api", "mpfs/office_action_data", map[string]interface{}{
		"editorParams": map[string]interface{}{
			"action": "edit", "idDoc": idDoc, "locale": "ru", "oo_type": "desktop",
			"isWopi": true, "idClient": "disk", "ext": "docx", "filename": name,
		},
		"source": "docs",
	}); err != nil {
		return "", fmt.Errorf("office_action_data: %w", err)
	}

	// 3) read the created file: resource_id (for sharing). office_online_sharing_url
	// also appears here, but set-public's response is the reliable source.
	info, err := volgaAPI(ctx, client, sk, "models-v2", "mpfs/info", map[string]interface{}{"path": path})
	if err != nil {
		return "", fmt.Errorf("info: %w", err)
	}
	rid := volgaResourceIDRe.FindSubmatch(info)
	if rid == nil {
		return "", fmt.Errorf("документ не создался: office_action_data не завёл файл %s (нет resource_id)", path)
	}
	resourceID := string(rid[1])

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

var (
	volgaSKRe         = regexp.MustCompile(`"sk"\s*:\s*"([0-9a-f]{20,}(?::\d+)?)"`)
	volgaResourceIDRe = regexp.MustCompile(`"resource_id"\s*:\s*"(\d+:[0-9a-f]{64})"`)
	volgaSharingRe    = regexp.MustCompile(`"office_online_sharing_url"\s*:\s*"([^"]+)"`)
)

// volgaSK fetches the authed docs.yandex.ru/docs page and reads the Disk sk
// token the API requires. An unauthenticated session redirects to passport and
// has no sk.
func volgaSK(ctx context.Context, client *http.Client) (string, error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", "https://docs.yandex.ru/docs", nil)
	req.Header.Set("User-Agent", boardsUA)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<21))
	if m := volgaSKRe.FindSubmatch(body); m != nil {
		return string(m[1]), nil
	}
	return "", fmt.Errorf("sk не найден (cookies не авторизованы?)")
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
