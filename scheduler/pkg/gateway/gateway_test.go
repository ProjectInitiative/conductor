package gateway

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/projectinitiative/conductor/pkg/scheduler"
)

func newTestEngine(models []scheduler.Model, backends map[string]string) *scheduler.Engine {
	return scheduler.NewEngine(models, backends)
}

func TestServeProxiesAndStreams(t *testing.T) {
	// Stub backend that echoes the request and streams a body back.
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Backend", "seen")
		io.WriteString(w, "BACKEND_RESPONSE")
	}))
	defer backend.Close()

	models := []scheduler.Model{{Name: "deepseek", Priority: 1, MaxRequests: 10, MaxSeconds: 600}}
	engine := newTestEngine(models, map[string]string{"deepseek": backend.URL})
	engine.StartTurn("deepseek")

	h := NewHandler(engine, nil)
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"deepseek","messages":[]}`))
	req.Header.Set("X-Test", "abc")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "BACKEND_RESPONSE") {
		t.Fatalf("expected proxied body, got %q", rec.Body.String())
	}
	if rec.Header().Get("X-Backend") != "seen" {
		t.Fatalf("expected upstream header passthrough, got %v", rec.Header())
	}
}

func TestParkWhenQuantumExhausted(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok")
	}))
	defer backend.Close()

	models := []scheduler.Model{{Name: "deepseek", Priority: 1, MaxRequests: 1}}
	engine := newTestEngine(models, map[string]string{"deepseek": backend.URL})
	engine.StartTurn("deepseek")
	engine.Tick() // one served -> quantum exhausted

	h := NewHandler(engine, nil)
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"deepseek"}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 park, got %d", rec.Code)
	}
	if engine.Len("deepseek") != 1 {
		t.Fatalf("expected request queued, got len=%d", engine.Len("deepseek"))
	}
}

func TestLoadWhenNotResident(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok")
	}))
	defer backend.Close()

	models := []scheduler.Model{{Name: "qwen", Priority: 2}}
	engine := newTestEngine(models, map[string]string{"qwen": backend.URL})

	h := NewHandler(engine, nil)
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"qwen"}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 load, got %d", rec.Code)
	}
	if engine.Len("qwen") != 1 {
		t.Fatalf("expected queued, got len=%d", engine.Len("qwen"))
	}
}

func TestUnknownModelNotFound(t *testing.T) {
	engine := newTestEngine(nil, nil)
	h := NewHandler(engine, nil)
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"nope"}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestModelsOnlyResident(t *testing.T) {
	engine := newTestEngine(
		[]scheduler.Model{{Name: "deepseek", Priority: 1}, {Name: "qwen", Priority: 2}},
		map[string]string{"deepseek": "http://x", "qwen": "http://y"},
	)
	engine.StartTurn("deepseek")
	h := NewHandler(engine, nil)
	req := httptest.NewRequest("GET", "/v1/models", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	var body struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("bad json: %v body=%s", err, rec.Body.String())
	}
	if len(body.Data) != 1 || body.Data[0].ID != "deepseek" {
		t.Fatalf("expected only resident deepseek, got %+v", body.Data)
	}
}

func TestStatusShape(t *testing.T) {
	engine := newTestEngine(
		[]scheduler.Model{{Name: "deepseek", Priority: 1}, {Name: "qwen", Priority: 2}},
		map[string]string{"deepseek": "http://x", "qwen": "http://y"},
	)
	engine.StartTurn("deepseek")
	engine.Enqueue("qwen", "req-1")
	h := NewHandler(engine, nil)
	req := httptest.NewRequest("GET", "/status", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	var st scheduler.Status
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		t.Fatalf("bad json: %v body=%s", err, rec.Body.String())
	}
	if st.Resident != "deepseek" {
		t.Fatalf("expected resident deepseek, got %q", st.Resident)
	}
	if st.Models["qwen"].Queued != 1 || st.Models["qwen"].Resident {
		t.Fatalf("expected qwen queued=1 not-resident, got %+v", st.Models["qwen"])
	}
}

func TestHealth(t *testing.T) {
	engine := newTestEngine(nil, nil)
	h := NewHandler(engine, nil)
	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}
