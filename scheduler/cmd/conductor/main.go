// conductor is the workload residency operator for scarce homelab resources:
//
//	conductor controller                  — Kubernetes-native workload operator
//	conductor lane <verb>                 — break-glass lane CLI (kubectl-backed)
package main

import (
	"fmt"
	"os"
)

// Version is injected at build time via ldflags (see flake.nix).
var Version = "dev"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	switch os.Args[1] {
	case "controller":
		runController()
	case "lane":
		if err := runLane(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "conductor lane: %v\n", err)
			os.Exit(1)
		}
	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [controller|lane <verb>]\n", os.Args[0])
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
