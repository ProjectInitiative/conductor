package k8s

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRealClientScaleDeployment(t *testing.T) {
	var scaled int32 = -1
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Fatalf("expected PATCH, got %s", r.Method)
		}
		if r.URL.Path != "/apis/apps/v1/namespaces/llm-test/deployments/head/scale" {
			t.Fatalf("bad path: %s", r.URL.Path)
		}
		var body struct {
			Spec struct {
				Replicas int32 `json:"replicas"`
			} `json:"spec"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		scaled = body.Spec.Replicas
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewRealClient(srv.URL, "tok", nil, "llm-test", srv.Client())
	if err := c.ScaleDeployment("llm-test", "head", 3); err != nil {
		t.Fatalf("scale: %v", err)
	}
	if scaled != 3 {
		t.Fatalf("expected replicas 3, got %d", scaled)
	}
}

func TestRealClientDeploymentGoneAndReady(t *testing.T) {
	status := "ready"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body any
		if status == "gone" {
			body = map[string]any{
				"spec":   map[string]any{"replicas": 0},
				"status": map[string]any{"replicas": 0, "readyReplicas": 0},
			}
		} else {
			body = map[string]any{
				"spec":   map[string]any{"replicas": 1},
				"status": map[string]any{"replicas": 1, "readyReplicas": 1, "availableReplicas": 1},
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	}))
	defer srv.Close()

	c := NewRealClient(srv.URL, "tok", nil, "llm-test", srv.Client())

	gone, err := c.DeploymentGone("llm-test", "head")
	if err != nil {
		t.Fatalf("gone: %v", err)
	}
	if gone {
		t.Fatal("expected not gone while ready")
	}
	status = "gone"
	gone, _ = c.DeploymentGone("llm-test", "head")
	if !gone {
		t.Fatal("expected gone after scale to 0")
	}
}

func TestRealClientWaitReadyPayload(t *testing.T) {
	notReady := map[string]any{
		"status": map[string]any{"replicas": 1, "readyReplicas": 0},
	}
	ready := map[string]any{
		"status": map[string]any{"replicas": 1, "readyReplicas": 1},
	}
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		body := notReady
		if calls >= 2 {
			body = ready
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	}))
	defer srv.Close()

	c := NewRealClient(srv.URL, "tok", nil, "llm-test", srv.Client())
	if err := c.WaitReady("llm-test", "head", 5*time.Second); err != nil {
		t.Fatalf("waitready: %v", err)
	}
	if calls < 2 {
		t.Fatalf("expected at least 2 polls, got %d", calls)
	}
}
