// Package fission exposes the same Conductor logic as Fission functions
// (multi-headed binary pattern): `conductor function status` and
// `conductor function model-ready`. Each sub-command runs a tiny HTTP handler.
package fission

import (
	"log"
	"net/http"
	"os"
)

// Run dispatches a Fission function sub-command.
func Run(args []string) {
	if len(args) == 0 {
		// Default: a generic function HTTP server.
		serve(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("{}"))
		})
		return
	}
	switch args[0] {
	case "status":
		serve(func(w http.ResponseWriter, r *http.Request) {
			// TODO(agent): return scheduler/status JSON.
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"resident":""}`))
		})
	case "model-ready":
		serve(func(w http.ResponseWriter, r *http.Request) {
			// TODO(agent): finalize a swap (update state + LiteLLM config).
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
		})
	default:
		log.Fatalf("unknown function sub-command: %s", args[0])
	}
}

func serve(h http.HandlerFunc) {
	port := os.Getenv("FISSION_FUNCTION_PORT")
	if port == "" {
		port = "8888"
	}
	log.Printf("conductor function listening on :%s", port)
	if err := http.ListenAndServe(":"+port, h); err != nil {
		log.Fatal(err)
	}
}


