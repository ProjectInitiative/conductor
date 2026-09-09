package fission

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/projectinitiative/conductor/pkg/scheduler"
)

type fakeStateRec struct {
	set    string
	called bool
}

func (f *fakeStateRec) SetState(model string) error { f.set = model; f.called = true; return nil }

func TestStatusHandler(t *testing.T) {
	engine := scheduler.NewEngine(
		[]scheduler.Model{{Name: "deepseek", Priority: 1}},
		map[string]string{"deepseek": "http://x"},
	)
	engine.StartTurn("deepseek")
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
	if st.Resident != "deepseek" {
		t.Fatalf("expected resident deepseek, got %q", st.Resident)
	}
}

func TestModelReadyFinalizesSwap(t *testing.T) {
	engine := scheduler.NewEngine(
		[]scheduler.Model{{Name: "qwen", Priority: 2}},
		map[string]string{"qwen": "http://x"},
	)
	state := &fakeStateRec{}
	srv := NewServer(engine, state)

	req := httptest.NewRequest("POST", "/model-ready?model=qwen", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if engine.Resident() != "qwen" {
		t.Fatalf("expected resident qwen after finalize, got %q", engine.Resident())
	}
	if !state.called || state.set != "qwen" {
		t.Fatalf("expected SetState(qwen), got %+v", state)
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
