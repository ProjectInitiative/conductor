// Package k8s implements the swap lifecycle against the cluster: scale down the
// old head/worker, scale up the target worker→head, poll /health, enforce
// single-flight mutual exclusion.
package k8s

import (
	"errors"
	"sync"
	"time"
)

// ErrBusy is returned when a swap is already in flight.
var ErrBusy = errors.New("swap already in progress")

// Client is the minimal Kubernetes surface Conductor needs. Hidden behind an
// interface so unit tests can drive the lifecycle with a fake.
type Client interface {
	ScaleDeployment(namespace, name string, replicas int32) error
	DeploymentGone(namespace, name string) (bool, error)
	WaitReady(namespace, name string, timeout time.Duration) error
	Health(hostIP string, port int, path string, timeout time.Duration) (bool, error)
	SetConfigMap(name string, data map[string]string) error
}

// Target describes a model's 2-node deployment for a swap.
type Target struct {
	HeadDeploy   string
	WorkerDeploy string
	Namespace    string
	HostIP       string
	Port         int
	HealthPath   string
	ReadyTimeout time.Duration
}

// Swapper performs an ordered swap lifecycle with single-flight exclusion.
type Swapper struct {
	Client Client
	mu     sync.Mutex
}

// NewSwapper builds a Swapper.
func NewSwapper(client Client) *Swapper { return &Swapper{Client: client} }

// Swap evacuates `from` (scale both to 0, wait gone), then loads `to` in
// worker→head order and polls the head's /health until ready. It returns
// ErrBusy if another swap is running. A nil `from` skips draining.
func (s *Swapper) Swap(from, to *Target) error {
	if !s.mu.TryLock() {
		return ErrBusy
	}
	defer s.mu.Unlock()

	if from != nil {
		if err := s.drain(from); err != nil {
			return err
		}
	}
	if to == nil {
		return nil
	}
	return s.load(to)
}

func (s *Swapper) drain(t *Target) error {
	// Scale down worker then head, waiting for each to disappear before the
	// other (shared fabric IPs/master ports must be fully released).
	if err := s.scale(t.WorkerDeploy, t.Namespace, 0); err != nil {
		return err
	}
	if err := s.waitGone(t.WorkerDeploy, t.Namespace); err != nil {
		return err
	}
	if err := s.scale(t.HeadDeploy, t.Namespace, 0); err != nil {
		return err
	}
	return s.waitGone(t.HeadDeploy, t.Namespace)
}

func (s *Swapper) load(t *Target) error {
	// Worker (rank 1) first, then head (rank 0) — matches the documented order.
	if err := s.scale(t.WorkerDeploy, t.Namespace, 1); err != nil {
		return err
	}
	if err := s.waitReady(t.WorkerDeploy, t.Namespace, t.ReadyTimeout); err != nil {
		return err
	}
	if err := s.scale(t.HeadDeploy, t.Namespace, 1); err != nil {
		return err
	}
	// Poll the head's host-network /health until ready.
	return s.waitHealth(t)
}

func (s *Swapper) scale(name, ns string, replicas int32) error {
	return s.Client.ScaleDeployment(ns, name, replicas)
}

func (s *Swapper) waitGone(name, ns string) error {
	timeout := time.Now().Add(5 * time.Minute)
	for {
		gone, err := s.Client.DeploymentGone(ns, name)
		if err != nil {
			return err
		}
		if gone {
			return nil
		}
		if time.Now().After(timeout) {
			return errors.New("timed out waiting for deployment to be gone: " + name)
		}
		time.Sleep(2 * time.Second)
	}
}

func (s *Swapper) waitReady(name, ns string, timeout time.Duration) error {
	if err := s.Client.WaitReady(ns, name, timeout); err != nil {
		return err
	}
	return nil
}

func (s *Swapper) waitHealth(t *Target) error {
	if t.ReadyTimeout <= 0 {
		t.ReadyTimeout = 30 * time.Minute
	}
	deadline := time.Now().Add(t.ReadyTimeout)
	path := t.HealthPath
	if path == "" {
		path = "/health"
	}
	for {
		ok, err := s.Client.Health(t.HostIP, t.Port, path, 10*time.Second)
		if err != nil {
			// Transient errors during load are not fatal; keep polling.
			ok = false
		}
		if ok {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("timed out waiting for " + t.HeadDeploy + " health")
		}
		time.Sleep(5 * time.Second)
	}
}
