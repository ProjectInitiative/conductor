package k8s

import "time"

// NoopClient is a no-op Client used in local dev (no cluster). All operations
// succeed without side effects so the driver/gateway can run standalone.
type NoopClient struct{}

func (NoopClient) ScaleDeployment(string, string, int32) error   { return nil }
func (NoopClient) DeploymentGone(string, string) (bool, error)   { return true, nil }
func (NoopClient) WaitReady(string, string, time.Duration) error { return nil }
func (NoopClient) Health(string, int, string, time.Duration) (bool, error) {
	return true, nil
}
func (NoopClient) SetConfigMap(string, map[string]string) error { return nil }
