// conductor is a multi-headed binary. Modeled on golden-template's
// compiled-serverless pattern:
//
//	conductor server                      — scheduler + traffic-light gateway (long-running)
//	conductor function status             — Fission function (status)
//	conductor function model-ready        — Fission function (swap finalize on Ready)
package main

import (
	"fmt"
	"os"

	"github.com/projectinitiative/conductor/pkg/fission"
	"github.com/projectinitiative/conductor/pkg/gateway"
	"github.com/projectinitiative/conductor/pkg/scheduler"
)

// Version is injected at build time via ldflags (see flake.nix).
var Version = "dev"

func main() {
	if len(os.Args) < 2 {
		runServer()
		return
	}
	switch os.Args[1] {
	case "server":
		runServer()
	case "function":
		// A dedicated engine for the function surface (models would be loaded
		// from the conductor-model-profiles ConfigMap at runtime).
		engine := scheduler.NewEngine(nil, nil)
		fission.NewServer(engine, nil).Run(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "Usage: %s [server|function <op>]\n", os.Args[0])
		os.Exit(1)
	}
}

func runServer() {
	gateway.Run(gateway.Config{})
}
