// Package fission exposes the same Conductor logic as Fission functions
// (multi-headed binary pattern): `conductor function status` and
// `conductor function model-ready`. Each sub-command runs a tiny HTTP server
// that dispatches on the request path.
package fission

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/projectinitiative/conductor/pkg/config"
	"github.com/projectinitiative/conductor/pkg/k8s"
	"github.com/projectinitiative/conductor/pkg/scheduler"
)

// Run is the Fission function entry point: it builds a Server (engine from
// model profiles + a best-effort state setter) and serves. The op is carried by
// the request path (/status, /model-ready).
func Run(args []string) {
	engine := engineOrEmpty()
	state := &stateSetter{client: clientOrNoop()}
	srv := NewServer(engine, state)
	srv.Run(args)
}

func engineOrEmpty() *scheduler.Engine {
	res, err := config.LoadFile(os.Getenv("CONDUCTOR_PROFILES"))
	if err != nil {
		log.Printf("no profiles (%v); serving empty engine", err)
		res = &config.Result{}
	}
	return scheduler.NewEngine(res.Models, res.Backends)
}

func clientOrNoop() k8s.Client {
	ns := os.Getenv("CONDUCTOR_NAMESPACE")
	if ns == "" {
		ns = "llm-test"
	}
	if c, err := k8s.NewInCluster(ns); err == nil {
		return c
	}
	return k8s.NoopClient{}
}

// stateSetter persists a finalized residency decision to the state ConfigMap.
type stateSetter struct {
	client k8s.Client
}

func (s *stateSetter) SetState(model string) error {
	if s.client == nil {
		return nil
	}
	return s.client.SetConfigMap("conductor-state", map[string]string{"resident": model})
}

// StateSetter persists a finalized residency decision (e.g. writes the model
// state ConfigMap and updates LiteLLM config). Hidden behind an interface so
// unit tests can fake it.
type StateSetter interface {
	SetState(model string) error
}

// Server is the Fission function surface.
type Server struct {
	Engine *scheduler.Engine
	State  StateSetter
}

// NewServer builds a Server.
func NewServer(engine *scheduler.Engine, state StateSetter) *Server {
	return &Server{Engine: engine, State: state}
}

// ServeHTTP dispatches on the request path.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/status":
		writeJSON(w, http.StatusOK, s.Engine.Status())
	case "/model-ready":
		s.handleModelReady(w, r)
	default:
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "unknown path"})
	}
}

func (s *Server) handleModelReady(w http.ResponseWriter, r *http.Request) {
	model, isWorker := modelFromRequest(r)
	if isWorker {
		// A worker becomes Ready before the head; never finalize on it.
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "ignored": "worker"})
		return
	}
	if model == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "model required"})
		return
	}
	s.Engine.StartTurn(model)
	if s.State != nil {
		if err := s.State.SetState(model); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "resident": model})
}

// modelFromRequest derives the model for a model-ready invocation from (in
// priority order) the ?model= query param, a JSON body {"model": ...}, or a
// Deployment watch payload (metadata.labels["conductor.io/model"]). It also
// reports whether the event is for a worker Deployment (name/app ending in
// "-worker"), which must be ignored.
func modelFromRequest(r *http.Request) (model string, isWorker bool) {
	if model = r.URL.Query().Get("model"); model != "" {
		return model, false
	}
	b, _ := io.ReadAll(r.Body)
	if len(b) == 0 {
		return "", false
	}
	// Simple {"model": ...} body.
	if model = fieldString(b, "model"); model != "" {
		return model, false
	}
	// Deployment watch payload.
	_, model, isWorker = deploymentFromBody(b)
	return model, isWorker
}

// deploymentFromBody extracts a Deployment's name and model label from its JSON
// watch payload, unwrapping a Fission "object" envelope if present.
func deploymentFromBody(b []byte) (name, model string, isWorker bool) {
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		return "", "", false
	}
	obj := raw
	if inner, ok := raw["object"].(map[string]any); ok {
		obj = inner
	}
	md, _ := obj["metadata"].(map[string]any)
	name, _ = md["name"].(string)
	labels, _ := md["labels"].(map[string]any)
	model, _ = labels["conductor.io/model"].(string)
	app, _ := labels["app"].(string)
	isWorker = strings.HasSuffix(name, "-worker") || strings.HasSuffix(app, "-worker")
	return name, model, isWorker
}

// fieldString reads a top-level string field from JSON.
func fieldString(b []byte, key string) string {
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return ""
	}
	v, _ := m[key].(string)
	return v
}

// Run starts a blocking HTTP function server on FISSION_FUNCTION_PORT (default
// 8888). It serves all handlers.
func (s *Server) Run(args []string) {
	port := os.Getenv("FISSION_FUNCTION_PORT")
	if port == "" {
		port = "8888"
	}
	log.Printf("conductor function listening on :%s", port)
	if err := http.ListenAndServe(":"+port, s); err != nil {
		log.Fatal(err)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
