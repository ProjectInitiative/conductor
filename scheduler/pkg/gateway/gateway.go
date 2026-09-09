// Package gateway implements Conductor's HTTP data plane: it applies the
// scheduler policy (traffic-light admission + stream-hold) and proxies
// OpenAI-compatible requests to the resident vLLM backend.
package gateway

import (
	"log"
	"net/http"

	"github.com/projectinitiative/conductor/pkg/scheduler"
)

// Config wires the gateway to its dependencies.
type Config struct {
	ListenAddr string
	Policy     *scheduler.Policy
	Backends   map[string]string // model -> resident backend base URL
}

// Run starts the HTTP gateway server (blocking).
func Run(cfg Config) {
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":8000"
	}
	if cfg.Policy == nil {
		cfg.Policy = scheduler.NewPolicy(scheduler.RealClock{}, nil)
	}
	mux := http.NewServeMux()

	// TODO(agent): wire admission + stream proxy + /status + /health handlers.

	log.Printf("conductor gateway listening on %s", cfg.ListenAddr)
	if err := http.ListenAndServe(cfg.ListenAddr, mux); err != nil {
		log.Fatal(err)
	}
}
