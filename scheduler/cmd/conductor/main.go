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
)

// Version is injected at build time via ldflags (see flake.nix).
var Version = "dev"

func main() {
	if len(os.Args) < 2 {
		gateway.Run(gateway.Config{})
		return
	}
	switch os.Args[1] {
	case "server":
		gateway.Run(gateway.Config{})
	case "function":
		fission.Run(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "Usage: %s [server|function <op>]\n", os.Args[0])
		os.Exit(1)
	}
}
