// Package dashboard serves QuorumLog's single-page web dashboard.
//
// The dashboard is plain HTML, CSS and JavaScript embedded in the binary
// (no build step, no external CDNs). It talks only to the same-origin HTTP
// API described in docs/SCOPE.md (/v1/...), so the API server mounts it as
// the catch-all handler:
//
//	mux.Handle("/", dashboard.Handler())
//
// More specific patterns such as "/v1/" and "/metrics" take precedence in an
// http.ServeMux, so the dashboard never shadows the API.
package dashboard

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"io/fs"
	"net/http"
	"path"
	"strings"
	"time"
)

//go:embed static
var staticFiles embed.FS

// contentTypes is set explicitly so responses do not depend on the host's
// mime.types file (some Linux distributions map .js to application/javascript).
var contentTypes = map[string]string{
	".html": "text/html; charset=utf-8",
	".css":  "text/css; charset=utf-8",
	".js":   "text/javascript; charset=utf-8",
	".svg":  "image/svg+xml",
	".json": "application/json",
	".txt":  "text/plain; charset=utf-8",
}

// csp allows only same-origin scripts, styles and API calls. The favicon is
// an inline data: SVG, hence img-src data:.
const csp = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; " +
	"connect-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'"

type asset struct {
	body        []byte
	etag        string
	contentType string
}

type handler struct {
	assets map[string]asset // key: path relative to static/, e.g. "index.html"
}

// Handler returns an http.Handler serving the embedded dashboard. "/" serves
// index.html; every other path maps to a file under static/. Unknown paths
// get 404, methods other than GET and HEAD get 405.
func Handler() http.Handler {
	sub, err := fs.Sub(staticFiles, "static")
	if err != nil {
		panic("dashboard: " + err.Error()) // impossible: the directory is embedded
	}
	h := &handler{assets: make(map[string]asset)}
	err = fs.WalkDir(sub, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := fs.ReadFile(sub, p)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(b)
		ct, ok := contentTypes[path.Ext(p)]
		if !ok {
			ct = http.DetectContentType(b)
		}
		h.assets[p] = asset{body: b, etag: `"` + hex.EncodeToString(sum[:8]) + `"`, contentType: ct}
		return nil
	})
	if err != nil {
		panic("dashboard: " + err.Error())
	}
	return h
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	name := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
	if name == "" {
		name = "index.html"
	}
	a, ok := h.assets[name]
	if !ok {
		http.NotFound(w, r)
		return
	}
	hdr := w.Header()
	hdr.Set("Content-Type", a.contentType)
	hdr.Set("ETag", a.etag)
	// Revalidate on every load so a redeploy is picked up immediately; the
	// ETag makes the revalidation a cheap 304.
	hdr.Set("Cache-Control", "no-cache")
	hdr.Set("X-Content-Type-Options", "nosniff")
	hdr.Set("Referrer-Policy", "no-referrer")
	if strings.HasSuffix(name, ".html") {
		hdr.Set("Content-Security-Policy", csp)
	}
	// ServeContent handles If-None-Match, Range and HEAD.
	http.ServeContent(w, r, name, time.Time{}, bytes.NewReader(a.body))
}
