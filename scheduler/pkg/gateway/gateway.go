// Package gateway implements Conductor's HTTP data plane: it applies the
// scheduler policy (traffic-light admission + stream-hold) and proxies
// OpenAI-compatible requests to the resident vLLM backend.
package gateway

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/projectinitiative/conductor/pkg/scheduler"
)

// Config wires the gateway to its dependencies.
type Config struct {
	ListenAddr string
	Models     []scheduler.Model
	Backends   map[string]string // model -> resident backend base URL
	CallerID   func(r *http.Request) string
}

// Handler is the HTTP data plane. It applies the scheduler policy per request
// and proxies admitted OpenAI-compatible requests to the owning backend.
type Handler struct {
	engine *scheduler.Engine
	client *http.Client
	caller func(r *http.Request) string
}

// NewHandler builds a gateway Handler from an Engine.
func NewHandler(engine *scheduler.Engine, caller func(r *http.Request) string) *Handler {
	if caller == nil {
		caller = func(r *http.Request) string { return "unknown" }
	}
	return &Handler{
		engine: engine,
		client: &http.Client{Timeout: 0}, // no client timeout: long streaming generations
		caller: caller,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/health":
		h.handleHealth(w, r)
		return
	case "/status":
		h.handleStatus(w, r)
		return
	case "/v1/models":
		h.handleModels(w, r)
		return
	case "/v1/chat/completions", "/v1/completions", "/v2/chat/completions":
		h.handleChat(w, r)
		return
	default:
		http.NotFound(w, r)
	}
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// StatusResponse is the shape returned by GET /status.
type StatusResponse struct {
	scheduler.Status
}

func (h *Handler) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.engine.Status())
}

func (h *Handler) handleModels(w http.ResponseWriter, r *http.Request) {
	resident := h.engine.Resident()
	if resident == "" {
		writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": []any{}})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"data": []map[string]any{
			{"id": resident, "object": "model"},
		},
	})
}

func (h *Handler) handleChat(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("bad_request", "could not read request body"))
		return
	}
	model := extractModel(body)
	if model == "" {
		writeJSON(w, http.StatusBadRequest, errBody("bad_request", "missing model field"))
		return
	}
	if _, ok := h.engine.Backend(model); !ok {
		writeJSON(w, http.StatusNotFound, errBody("model_not_found", "unknown model: "+model))
		return
	}

	reqID := h.caller(r)
	switch h.engine.Admit(model) {
	case scheduler.Serve:
		h.engine.Tick()
		h.proxy(w, r, model, body, reqID)
	case scheduler.Park:
		h.engine.Enqueue(model, reqID)
		writeJSON(w, http.StatusServiceUnavailable, errBody("model_busy",
			"model "+model+" is resident but its green window is closed; queued"))
	case scheduler.Load:
		h.engine.Enqueue(model, reqID)
		writeJSON(w, http.StatusServiceUnavailable, errBody("model_loading",
			"model "+model+" is loading; queued for its next turn"))
	}
}

// proxy forwards an admitted request to the model's backend and streams the
// response body back verbatim (SSE streaming preserved).
func (h *Handler) proxy(w http.ResponseWriter, r *http.Request, model string, body []byte, reqID string) {
	backend, ok := h.engine.Backend(model)
	if !ok {
		writeJSON(w, http.StatusNotFound, errBody("model_not_found", "unknown model: "+model))
		return
	}
	outReq, err := http.NewRequestWithContext(r.Context(), r.Method, backend+r.URL.Path, bytes.NewReader(body))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("proxy_error", err.Error()))
		return
	}
	// Copy headers (skip hop-by-hop and host).
	for k, vals := range r.Header {
		if isHopHeader(k) || k == "Host" {
			continue
		}
		outReq.Header[k] = vals
	}
	outReq.Header.Set("X-Conductor-Request-ID", reqID)
	outReq.Header.Set("X-Conductor-Resident", model)

	resp, err := h.client.Do(outReq)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, errBody("backend_unreachable", err.Error()))
		return
	}
	defer resp.Body.Close()
	for k, vals := range resp.Header {
		w.Header()[k] = vals
	}
	w.WriteHeader(resp.StatusCode)
	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 32*1024)
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			w.Write(buf[:n])
			if flusher != nil {
				flusher.Flush()
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			break
		}
	}
}

func extractModel(body []byte) string {
	var req struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return ""
	}
	return req.Model
}

func errBody(code, msg string) map[string]any {
	return map[string]any{
		"error": map[string]any{
			"message": msg,
			"type":    code,
			"code":    code,
		},
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func isHopHeader(k string) bool {
	switch http.CanonicalHeaderKey(k) {
	case "Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
		"Te", "Trailer", "Transfer-Encoding", "Upgrade":
		return true
	}
	return false
}

// Run builds a gateway from Config and serves until signal.
func Run(cfg Config) {
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":8000"
	}
	engine := scheduler.NewEngine(cfg.Models, cfg.Backends)
	handler := NewHandler(engine, cfg.CallerID)
	log.Printf("conductor gateway listening on %s", cfg.ListenAddr)
	srv := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      handler,
		ReadTimeout:  120 * time.Second,
		WriteTimeout: 0,
	}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
