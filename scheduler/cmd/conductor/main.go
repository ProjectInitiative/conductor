// conductor is a multi-headed binary. Modeled on golden-template's
// compiled-serverless pattern:
//
//	conductor server                      — scheduler + traffic-light gateway (long-running)
//	conductor function status             — Fission function (status)
//	conductor function model-ready        — Fission function (swap finalize on Ready)
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/projectinitiative/conductor/pkg/fission"
	"github.com/projectinitiative/conductor/pkg/gateway"
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
	case "lane":
		if err := runLane(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "conductor lane: %v\n", err)
			os.Exit(1)
		}
	case "function":
		fission.Run(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "Usage: %s [server|lane <verb>|function <op>]\n", os.Args[0])
		os.Exit(1)
	}
}

func runServer() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	opts := gateway.Options{
		ListenAddr:   envOr("CONDUCTOR_LISTEN", ":8000"),
		ProfilesPath: envOr("CONDUCTOR_PROFILES", "/etc/conductor/profiles/models.yaml"),
		Namespace:    envOr("CONDUCTOR_NAMESPACE", "llm-test"),
	}
	if err := gateway.Run(ctx, opts); err != nil {
		fmt.Fprintf(os.Stderr, "conductor: %v\n", err)
		os.Exit(1)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
