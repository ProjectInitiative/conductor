package fission

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/projectinitiative/conductor/pkg/scheduler"
)

type fakeStateRec struct {
	set    string
	called bool
}

func (f *fakeStateRec) SetState(model string) error { f.set = model; f.called = true; return nil }

func newTestEngine(model string) *scheduler.Engine {
	return scheduler.NewEngine(
		[]scheduler.Model{{Name: model, Priority: 1}},
		map[string]string{model: "http://x"},
	)
}

func TestStatusHandler(t *testing.T) {
	engine := newTestEngine("deepseek-v4-flash-vision-exp")
	engine.StartTurn("deepseek-v4-flash-vision-exp")
	srv := NewServer(engine, nil)

	req := httptest.NewRequest("GET", "/status", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var st scheduler.Status
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	if st.Resident != "deepseek-v4-flash-vision-exp" {
		t.Fatalf("expected resident deepseek, got %q", st.Resident)
	}
}

func TestModelReadyFinalizesSwap(t *testing.T) {
	engine := newTestEngine("qwen3.8-flash-next")
	state := &fakeStateRec{}
	srv := NewServer(engine, state)

	req := httptest.NewRequest("POST", "/model-ready?model=qwen3.8-flash-next", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if engine.Resident() != "qwen3.8-flash-next" {
		t.Fatalf("expected resident after finalize, got %q", engine.Resident())
	}
	if !state.called || state.set != "qwen3.8-flash-next" {
		t.Fatalf("expected SetState, got %+v", state)
	}
}

func TestModelReadyRequiresModel(t *testing.T) {
	engine := scheduler.NewEngine(nil, nil)
	srv := NewServer(engine, nil)
	req := httptest.NewRequest("POST", "/model-ready", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing model, got %d", rec.Code)
	}
}

// deploymentEvent builds a Fission-like Kubernetes watch payload for a
// Deployment with the given metadata, wrapped under "object" like Fission's
// kubewatcher does for some versions.
func deploymentEvent(name string, labels map[string]string) string {
	obj := map[string]any{
		"kind": "Deployment",
		"metadata": map[string]any{
			"name":   name,
			"labels": labels,
		},
	}
	b, _ := json.Marshal(map[string]any{"object": obj})
	return string(b)
}

func TestModelReadyFromDeploymentPayload(t *testing.T) {
	engine := newTestEngine("qwen3.8-flash-next")
	srv := NewServer(engine, &fakeStateRec{})

	body := deploymentEvent("qwen-parity-head", map[string]string{
		"conductor.io/model": "qwen3.8-flash-next",
		"conductor.io/role":  "head",
		"app":                "qwen-parity-head",
	})
	req := httptest.NewRequest("POST", "/model-ready", strings.NewReader(body))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if engine.Resident() != "qwen3.8-flash-next" {
		t.Fatalf("expected resident from label, got %q", engine.Resident())
	}
}

func TestModelReadyIgnoresWorker(t *testing.T) {
	engine := newTestEngine("qwen3.8-flash-next")
	srv := NewServer(engine, nil)

	body := deploymentEvent("qwen-parity-worker", map[string]string{
		"conductor.io/model": "qwen3.8-flash-next",
		"conductor.io/role":  "worker",
		"app":                "qwen-parity-worker",
	})
	req := httptest.NewRequest("POST", "/model-ready", strings.NewReader(body))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for ignored worker, got %d", rec.Code)
	}
	if engine.Resident() != "" {
		t.Fatalf("expected no residency for worker event, got %q", engine.Resident())
	}
}

func TestModelReadyFromQueryFallback(t *testing.T) {
	engine := newTestEngine("deepseek-v4-flash-vision-exp")
	srv := NewServer(engine, nil)
	req := httptest.NewRequest("POST", "/model-ready?model=deepseek-v4-flash-vision-exp", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if engine.Resident() != "deepseek-v4-flash-vision-exp" {
		t.Fatalf("expected resident from query, got %q", engine.Resident())
	}
}
