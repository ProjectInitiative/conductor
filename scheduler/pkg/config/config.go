// Package config loads Conductor's model profiles (from the
// conductor-model-profiles ConfigMap / a mounted YAML file) into the scheduler
// model set plus the gateway's backend map and the swap deployment names.
package config

import (
	"fmt"
	"io"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/projectinitiative/conductor/pkg/scheduler"
)

// Deploy addresses one node of a model.
type Deploy struct {
	Deployment string `yaml:"deployment"`
	HostIP     string `yaml:"hostIP"`
	Port       int    `yaml:"port"`
}

// Health describes the readiness probe.
type Health struct {
	Path    string `yaml:"path"`
	Timeout string `yaml:"timeout"`
}

// Quantum is the traffic-light window for one turn.
type Quantum struct {
	MaxRequests int `yaml:"max_requests"`
	MaxSeconds  int `yaml:"max_seconds"`
}

// Profile is one model's scheduling + deployment profile.
type Profile struct {
	Name     string  `yaml:"name"`
	Priority int     `yaml:"priority"`
	Image    string  `yaml:"image"`
	Head     Deploy  `yaml:"head"`
	Worker   Deploy  `yaml:"worker"`
	Health   Health  `yaml:"health"`
	Quantum  Quantum `yaml:"quantum"`
	MinDwell string  `yaml:"min_dwell"`
	Cooldown string  `yaml:"cooldown"`
}

// ModelsFile is the on-disk shape of the profiles ConfigMap (models.yaml).
type ModelsFile struct {
	Models []Profile `yaml:"models"`
}

// Backend is the live endpoint for a model's head node.
type Backend struct {
	BaseURL string
	Deploy  struct {
		Head   string
		Worker string
	}
	HealthPath string
}

// Result is the loaded scheduling + wiring model.
type Result struct {
	Models   []scheduler.Model
	Backends map[string]string // model -> head base URL
	Deploys  map[string]struct {
		Head   string
		Worker string
	}
	Heads       map[string]Deploy // model -> head host/port (for /health and Target)
	HealthPaths map[string]string
}

// Load reads a ModelsFile from r and builds a Result.
func Load(r io.Reader) (*Result, error) {
	var mf ModelsFile
	dec := yaml.NewDecoder(r)
	if err := dec.Decode(&mf); err != nil {
		return nil, fmt.Errorf("decode profiles: %w", err)
	}
	res := &Result{
		Backends:    map[string]string{},
		Deploys:     map[string]struct{ Head, Worker string }{},
		Heads:       map[string]Deploy{},
		HealthPaths: map[string]string{},
	}
	for _, p := range mf.Models {
		minDwell, err := parseDuration(p.MinDwell)
		if err != nil {
			return nil, fmt.Errorf("model %s min_dwell: %w", p.Name, err)
		}
		cooldown, err := parseDuration(p.Cooldown)
		if err != nil {
			return nil, fmt.Errorf("model %s cooldown: %w", p.Name, err)
		}
		res.Models = append(res.Models, scheduler.Model{
			Name:        p.Name,
			Priority:    p.Priority,
			MaxRequests: p.Quantum.MaxRequests,
			MaxSeconds:  p.Quantum.MaxSeconds,
			MinDwell:    minDwell,
			Cooldown:    cooldown,
		})
		res.Backends[p.Name] = fmt.Sprintf("http://%s:%d", p.Head.HostIP, p.Head.Port)
		res.Deploys[p.Name] = struct{ Head, Worker string }{
			Head:   p.Head.Deployment,
			Worker: p.Worker.Deployment,
		}
		res.Heads[p.Name] = p.Head
		hp := p.Health.Path
		if hp == "" {
			hp = "/health"
		}
		res.HealthPaths[p.Name] = hp
	}
	return res, nil
}

// LoadFile loads profiles from a path (used by the gateway).
func LoadFile(path string) (*Result, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Load(f)
}

func parseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	return time.ParseDuration(s)
}
