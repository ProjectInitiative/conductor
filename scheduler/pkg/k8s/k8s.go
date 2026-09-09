// Package k8s implements the swap lifecycle against the cluster: scale down the
// old head/worker, scale up the target worker→head, poll /health, enforce
// single-flight mutual exclusion.
package k8s

import (
	"time"
)

// Client is the minimal Kubernetes surface Conductor needs. Hidden behind an
// interface so unit tests can drive the lifecycle with a fake.
type Client interface {
	ScaleDeployment(namespace, name string, replicas int32) error
	DeploymentGone(namespace, name string) (bool, error)
	Health(hostIP string, port int, path string, timeout time.Duration) (bool, error)
	SetConfigMap(name string, data map[string]string) error
}

// Swapper performs an ordered swap lifecycle.
type Swapper struct {
	Client Client
	// Configured per-model deploy names / host IP / port / etc.
}

// Swap drains `from` and loads `to`, returning once `to` is healthy.
func (s *Swapper) Swap(toDeploy, fromDeploy, namespace, hostIP string, port int, healthPath string, readyTimeout time.Duration) error {
	// TODO(agent): worker→head order, mutual exclusion, /health gate.
	_ = toDeploy
	_ = fromDeploy
	_ = namespace
	_ = hostIP
	_ = port
	_ = healthPath
	_ = readyTimeout
	return nil
}
