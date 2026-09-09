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

	"github.com/projectinitiative/conductor/pkg/scheduler"
)

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
	model := r.URL.Query().Get("model")
	if model == "" {
		var body struct {
			Model string `json:"model"`
		}
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &body)
		model = body.Model
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
