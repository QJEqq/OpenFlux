package yandex

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// toServer sends every request to srv while the client keeps seeing the real
// host (resp.Request is the original request), as it would against Yandex.
type toServer struct{ srv *httptest.Server }

func (s toServer) RoundTrip(req *http.Request) (*http.Response, error) {
	target, _ := url.Parse(s.srv.URL)
	r2 := req.Clone(req.Context())
	r2.URL.Scheme, r2.URL.Host = target.Scheme, target.Host
	resp, err := http.DefaultTransport.RoundTrip(r2)
	if resp != nil {
		resp.Request = req
	}
	return resp, err
}

func TestVolgaCreateFollowsTheEditorRedirect(t *testing.T) {
	var got url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/editnew/docx/disk" {
			got = r.URL.Query()
			http.Redirect(w, r, "https://docs.yandex.ru/edit/disk/disk%2F"+got.Get("filename")+".docx?sk=y1&source=docs", http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	client := &http.Client{Transport: toServer{srv}}

	path, err := volgaCreate(context.Background(), client, "y0123456789abcdef0123456789abcdef", "doc_0a1b2c")
	if err != nil {
		t.Fatal(err)
	}
	if path != "/disk/doc_0a1b2c.docx" {
		t.Fatalf("path %q", path)
	}
	if got.Get("sk") != "y0123456789abcdef0123456789abcdef" || got.Get("source") != "docs" || got.Get("filename") != "doc_0a1b2c" {
		t.Fatalf("query %v", got)
	}
}

func TestVolgaCreateReportsTheAuthWall(t *testing.T) {
	for name, to := range map[string]string{
		"passport": "https://passport.yandex.ru/auth?retpath=x",
		"captcha":  "https://disk.yandex.ru/showcaptcha?retpath=x",
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/editnew/docx/disk" {
				http.Redirect(w, r, to, http.StatusFound)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		_, err := volgaCreate(context.Background(), &http.Client{Transport: toServer{srv}}, "y0", "doc_x")
		srv.Close()
		if err == nil || strings.Contains(err.Error(), "retpath") {
			t.Errorf("%s: err %v (want an error without the query)", name, err)
		}
	}
}

func TestVolgaTokenPatterns(t *testing.T) {
	sk, ext := "0123456789abcdef0123456789abcdef01234567:1762560000", "y0123456789abcdef0123456789abcdef"
	for _, page := range []string{
		`{"sk":"` + sk + `","skExternal":"` + ext + `"}`,
		`JSON.parse("{\"sk\":\"` + sk + `\",\"skExternal\":\"` + ext + `\"}")`,
	} {
		if m := volgaSKRe.FindStringSubmatch(page); m == nil || m[1] != sk {
			t.Errorf("sk in %s: %v", page, m)
		}
		if m := volgaSKExternalRe.FindStringSubmatch(page); m == nil || m[1] != ext {
			t.Errorf("skExternal in %s: %v", page, m)
		}
	}
	if volgaSKRe.MatchString(`{"disk":"0123456789abcdef0123456789abcdef"}`) {
		t.Error(`"disk" is not "sk"`)
	}
}
