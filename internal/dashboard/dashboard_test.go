package dashboard

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func get(t *testing.T, h http.Handler, method, path string, hdr map[string]string) *http.Response {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Result()
}

func body(t *testing.T, r *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestHandlerServesAssets(t *testing.T) {
	h := Handler()
	cases := []struct {
		path, ctype, contains string
	}{
		{"/", "text/html; charset=utf-8", `<script src="app.js" defer></script>`},
		{"/index.html", "text/html; charset=utf-8", "<title>QuorumLog Dashboard</title>"},
		{"/app.js", "text/javascript; charset=utf-8", `"use strict";`},
		{"/app.css", "text/css; charset=utf-8", "prefers-color-scheme: dark"},
		{"/./app.js", "text/javascript; charset=utf-8", `"use strict";`},
	}
	for _, c := range cases {
		r := get(t, h, http.MethodGet, c.path, nil)
		if r.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: status %d", c.path, r.StatusCode)
		}
		if got := r.Header.Get("Content-Type"); got != c.ctype {
			t.Errorf("GET %s: Content-Type %q, want %q", c.path, got, c.ctype)
		}
		if got := r.Header.Get("X-Content-Type-Options"); got != "nosniff" {
			t.Errorf("GET %s: X-Content-Type-Options %q", c.path, got)
		}
		if b := body(t, r); !strings.Contains(b, c.contains) {
			t.Errorf("GET %s: body does not contain %q", c.path, c.contains)
		}
	}
}

func TestHandlerIndexHasCSPAndNoExternalResources(t *testing.T) {
	r := get(t, Handler(), http.MethodGet, "/", nil)
	if csp := r.Header.Get("Content-Security-Policy"); !strings.Contains(csp, "script-src 'self'") {
		t.Errorf("CSP = %q", csp)
	}
	b := body(t, r)
	for _, bad := range []string{"http://", "https://", "//cdn"} {
		// The SVG namespace inside the data: favicon is the only allowed URL.
		if strings.Contains(strings.ReplaceAll(b, "http://www.w3.org/2000/svg", ""), bad) {
			t.Errorf("index.html references an external resource (%q)", bad)
		}
	}
	for _, id := range []string{`id="guide"`, `id="nodes"`, `id="groups"`, `id="events"`, `id="produce-form"`, `id="tail"`, `id="offset-form"`} {
		if !strings.Contains(b, id) {
			t.Errorf("index.html is missing %s", id)
		}
	}
	if !strings.Contains(b, "resets when the free instance restarts") {
		t.Error("footer is missing the free-tier caveat")
	}
}

func TestHandlerNotFound(t *testing.T) {
	h := Handler()
	for _, p := range []string{"/missing", "/missing.js", "/static/app.js", "/../go.mod", "/v1/cluster", "/dashboard.go"} {
		r := get(t, h, http.MethodGet, p, nil)
		if r.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s: status %d, want 404", p, r.StatusCode)
		}
	}
}

func TestHandlerMethods(t *testing.T) {
	h := Handler()
	r := get(t, h, http.MethodPost, "/", nil)
	if r.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST /: status %d, want 405", r.StatusCode)
	}
	if got := r.Header.Get("Allow"); got != "GET, HEAD" {
		t.Errorf("Allow = %q", got)
	}
	r = get(t, h, http.MethodHead, "/app.css", nil)
	if r.StatusCode != http.StatusOK {
		t.Fatalf("HEAD /app.css: status %d", r.StatusCode)
	}
	if b := body(t, r); b != "" {
		t.Errorf("HEAD returned a body of %d bytes", len(b))
	}
}

func TestHandlerETag(t *testing.T) {
	h := Handler()
	r := get(t, h, http.MethodGet, "/app.js", nil)
	etag := r.Header.Get("ETag")
	if etag == "" || r.Header.Get("Cache-Control") != "no-cache" {
		t.Fatalf("ETag %q, Cache-Control %q", etag, r.Header.Get("Cache-Control"))
	}
	r = get(t, h, http.MethodGet, "/app.js", map[string]string{"If-None-Match": etag})
	if r.StatusCode != http.StatusNotModified {
		t.Errorf("conditional GET: status %d, want 304", r.StatusCode)
	}
}

// TestMountedWithAPI checks the intended wiring: the dashboard at "/" never
// shadows more specific API routes.
func TestMountedWithAPI(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/health", func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, `{"node_id":1,"ok":true}`)
	})
	mux.Handle("/", Handler())
	if b := body(t, get(t, mux, http.MethodGet, "/v1/health", nil)); !strings.Contains(b, `"ok":true`) {
		t.Errorf("/v1/health served %q", b)
	}
	if r := get(t, mux, http.MethodGet, "/", nil); r.StatusCode != http.StatusOK {
		t.Errorf("/ status %d", r.StatusCode)
	}
}
