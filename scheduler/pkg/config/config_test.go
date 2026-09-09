package config

import (
	"strings"
	"testing"
	"time"
)

const sample = `
models:
  - name: deepseek-v4-flash-vision-exp
    priority: 1
    head: {deployment: deepseek-parity-head, hostIP: 172.16.4.55, port: 8000}
    worker: {deployment: deepseek-parity-worker, hostIP: 172.16.4.56}
    health: {path: /health, timeout: 3600s}
    quantum: {max_requests: 32, max_seconds: 600}
    min_dwell: 900s
    cooldown: 300s
  - name: qwen3.8-flash-next
    priority: 3
    head: {deployment: qwen-parity-head, hostIP: 172.16.4.55, port: 8888}
    worker: {deployment: qwen-parity-worker, hostIP: 172.16.4.56}
    health: {path: /health}
    quantum: {max_requests: 16, max_seconds: 600}
    min_dwell: 900s
    cooldown: 300s
`

func TestLoad(t *testing.T) {
	res, err := Load(strings.NewReader(sample))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(res.Models) != 2 {
		t.Fatalf("expected 2 models, got %d", len(res.Models))
	}
	// Order preserved; priority field parsed.
	if res.Models[0].Name != "deepseek-v4-flash-vision-exp" || res.Models[0].Priority != 1 {
		t.Fatalf("unexpected first model: %+v", res.Models[0])
	}
	if res.Models[0].MaxRequests != 32 || res.Models[0].MaxSeconds != 600 {
		t.Fatalf("quantum not parsed: %+v", res.Models[0])
	}
	if res.Models[0].MinDwell != 900*time.Second || res.Models[0].Cooldown != 300*time.Second {
		t.Fatalf("durations not parsed: %+v", res.Models[0])
	}
	// Backends keyed by model name.
	if res.Backends["deepseek-v4-flash-vision-exp"] != "http://172.16.4.55:8000" {
		t.Fatalf("bad backend: %q", res.Backends["deepseek-v4-flash-vision-exp"])
	}
	if res.Deploys["qwen3.8-flash-next"].Head != "qwen-parity-head" {
		t.Fatalf("bad deploy: %+v", res.Deploys["qwen3.8-flash-next"])
	}
	// Default health path.
	if res.HealthPaths["qwen3.8-flash-next"] != "/health" {
		t.Fatalf("bad health path: %q", res.HealthPaths["qwen3.8-flash-next"])
	}
}

func TestLoadDefaultHealthPath(t *testing.T) {
	if _, err := Load(strings.NewReader(sample)); err != nil {
		t.Fatal(err)
	}
}
