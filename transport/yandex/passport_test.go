package yandex

import (
	"errors"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
)

// fakePassport is a document at /edit/d/doc that sends a visitor to
// Passport's /auth/update until they bring a reissued Session_id ("fresh"),
// and a Passport whose /auth/update answers with refresh. Passport sits on
// the same host under /passport.yandex/…, which is what the transports look
// for in a redirect.
type fakePassport struct {
	srv       *httptest.Server
	refreshes atomic.Int32 // requests to /auth/update
	sawStale  atomic.Bool  // /auth/update got the sign-in it is to refresh
}

func newFakePassport(t *testing.T, refresh func(w http.ResponseWriter, r *http.Request)) *fakePassport {
	t.Helper()
	f := &fakePassport{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/edit/d/doc":
			if c, err := r.Cookie("Session_id"); err == nil && c.Value == "fresh" {
				_, _ = w.Write([]byte("<html>the editor</html>"))
				return
			}
			back := url.QueryEscape(f.srv.URL + "/edit/d/doc?source=docs")
			http.Redirect(w, r, "/passport.yandex/auth/update/?from=cloud&origin=disk_docs_ru&retpath="+back, http.StatusFound)
		case strings.HasPrefix(r.URL.Path, "/passport.yandex/auth/update"):
			f.refreshes.Add(1)
			if c, err := r.Cookie("Session_id"); err == nil && c.Value == "stale" {
				f.sawStale.Store(true)
			}
			refresh(w, r)
		case r.URL.Path == "/passport.yandex/auth":
			_, _ = w.Write([]byte("<html>login form</html>"))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

// jar holds the sign-in Docs wants refreshed.
func (f *fakePassport) jar() *cookiejar.Jar {
	jar, _ := cookiejar.New(nil)
	u, _ := url.Parse(f.srv.URL)
	jar.SetCookies(u, []*http.Cookie{{Name: "Session_id", Value: "stale", Path: "/"}})
	return jar
}

func (f *fakePassport) docURL() string { return f.srv.URL + "/edit/d/doc?source=docs" }

// reissue is Passport refreshing the sign-in and sending the visitor back.
func reissue(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{Name: "Session_id", Value: "fresh", Path: "/"})
	http.Redirect(w, r, r.URL.Query().Get("retpath"), http.StatusFound)
}

func cookieValue(cs []*http.Cookie, name string) string {
	for _, c := range cs {
		if c.Name == name {
			return c.Value
		}
	}
	return ""
}

func TestIsPassportRefresh(t *testing.T) {
	for loc, want := range map[string]bool{
		"https://passport.yandex.ru/auth/update/?from=cloud&origin=disk_docs_ru&retpath=https%3A%2F%2Fdocs.yandex.ru%2Fedit%2Fd%2Fx%3Fsource%3Ddocs": true,
		"https://passport.yandex.ru/auth/update?retpath=x":                                         true,
		"/passport.yandex/auth/update/?retpath=x":                                                  true,
		"https://passport.yandex.ru/auth?retpath=https%3A%2F%2Fpassport.yandex.ru%2Fauth%2Fupdate": false,
		"https://passport.yandex.ru/auth/welcome?retpath=x":                                        false,
		"https://docs.yandex.ru/auth/update":                                                       false,
	} {
		if got := isPassportRefresh(loc); got != want {
			t.Errorf("isPassportRefresh(%q) = %v, want %v", loc, got, want)
		}
	}
}

// The exit's case: Docs wants the operator's sign-in refreshed. The volga
// transport follows /auth/update with its cookies, keeps the reissued
// Session_id and goes back to the document instead of reporting a login wall.
func TestVolgaFollowsPassportSessionRefresh(t *testing.T) {
	f := newFakePassport(t, reissue)
	jar := f.jar()
	_, err := authorizeWithJar(f.docURL(), jar)
	if errors.Is(err, ErrLoginRequired) {
		t.Fatalf("a session refresh was taken for a login wall: %v", err)
	}
	// The fake editor has no client-config: failing on it means the
	// transport got back to the document past Passport.
	if err == nil || !strings.Contains(err.Error(), "client-config not found") {
		t.Fatalf("expected to reach the editor page, got %v", err)
	}
	if n := f.refreshes.Load(); n != 1 || !f.sawStale.Load() {
		t.Fatalf("Passport: %d refreshes, got the sign-in %v; want 1, true", n, f.sawStale.Load())
	}
	u, _ := url.Parse(f.srv.URL)
	if got := cookieValue(jar.Cookies(u), "Session_id"); got != "fresh" {
		t.Fatalf("the jar keeps Session_id=%q, want the reissued one", got)
	}
}

// A refresh that does not take (Docs asks again) is the login wall, not a loop.
func TestVolgaFollowsPassportRefreshOnce(t *testing.T) {
	f := newFakePassport(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, r.URL.Query().Get("retpath"), http.StatusFound) // nothing reissued
	})
	_, err := authorizeWithJar(f.docURL(), f.jar())
	var pe *PromptError
	if !errors.Is(err, ErrLoginRequired) || !errors.As(err, &pe) || !strings.Contains(pe.URL, "/auth/update") {
		t.Fatalf("expected ErrLoginRequired at /auth/update, got %v", err)
	}
	if n := f.refreshes.Load(); n != 1 {
		t.Fatalf("Passport was asked %d times, want 1", n)
	}
}

// A sign-in Passport will not refresh ends on the login form (a redirect to
// it, or a page of its own): that is the wall, and the page to show.
func TestVolgaPassportRefreshEndsOnLoginWall(t *testing.T) {
	for name, tc := range map[string]struct {
		refresh func(w http.ResponseWriter, r *http.Request)
		prompt  string
	}{
		"redirect to the login form": {func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/passport.yandex/auth?retpath="+url.QueryEscape(r.URL.Query().Get("retpath")), http.StatusFound)
		}, "/passport.yandex/auth?retpath="},
		"a page of its own": {func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("<html>confirm it is you</html>"))
		}, "/passport.yandex/auth/update/"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFakePassport(t, tc.refresh)
			_, err := authorizeWithJar(f.docURL(), f.jar())
			var pe *PromptError
			if !errors.Is(err, ErrLoginRequired) || !errors.As(err, &pe) || !strings.Contains(pe.URL, tc.prompt) {
				t.Fatalf("expected ErrLoginRequired at %s, got %v", tc.prompt, err)
			}
		})
	}
}

// The docs transport passes the refresh the same way.
func TestFetchDocInfoFollowsPassportSessionRefresh(t *testing.T) {
	f := newFakePassport(t, reissue)
	tr := &YandexDocsTransport{url: f.docURL(), cookieJar: f.jar()}
	_, err := tr.fetchDocInfo(f.docURL(), "0000000001")
	if err == nil || errors.Is(err, ErrLoginRequired) {
		t.Fatalf("expected to get past Passport to the (configless) editor, got %v", err)
	}
	if n := f.refreshes.Load(); n != 1 || !f.sawStale.Load() {
		t.Fatalf("Passport: %d refreshes, got the sign-in %v; want 1, true", n, f.sawStale.Load())
	}
}
