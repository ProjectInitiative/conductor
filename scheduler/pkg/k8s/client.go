package k8s

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

const (
	serviceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"
	tokenFile         = serviceAccountDir + "/token"
	caFile            = serviceAccountDir + "/ca.crt"
)

// RealClient talks to the Kubernetes API over HTTPS using the in-cluster
// service-account token/ca. It uses only the standard library so it builds and
// tests without a network.
type RealClient struct {
	client    *http.Client
	base      string // e.g. https://kubernetes.default.svc
	token     string
	Namespace string // default namespace for SetConfigMap
}

// NewInCluster builds a RealClient from the in-cluster service account.
func NewInCluster(namespace string) (*RealClient, error) {
	token, err := os.ReadFile(tokenFile)
	if err != nil {
		return nil, fmt.Errorf("read service account token: %w", err)
	}
	ca, err := os.ReadFile(caFile)
	if err != nil {
		// ca.crt optional in some clusters; proceed without it.
		ca = nil
	}
	return NewRealClient("https://kubernetes.default.svc", string(token), ca, namespace, nil), nil
}

// NewRealClient builds a RealClient from explicit params (also used by tests).
func NewRealClient(base, token string, ca []byte, namespace string, hc *http.Client) *RealClient {
	if hc == nil {
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if len(ca) > 0 {
			pool := x509.NewCertPool()
			pool.AppendCertsFromPEM(ca)
			tlsCfg.RootCAs = pool
		}
		hc = &http.Client{
			Transport: &http.Transport{TLSClientConfig: tlsCfg},
			Timeout:   30 * time.Second,
		}
	}
	return &RealClient{
		client:    hc,
		base:      base,
		token:     token,
		Namespace: namespace,
	}
}

// ScaleDeployment scales a Deployment replicas via the /scale subresource.
func (c *RealClient) ScaleDeployment(ns, name string, replicas int32) error {
	body := map[string]any{"spec": map[string]any{"replicas": replicas}}
	return c.do("PATCH", c.base+"/apis/apps/v1/namespaces/"+ns+"/deployments/"+name+"/scale", body)
}

func (c *RealClient) DeploymentGone(ns, name string) (bool, error) {
	var d struct {
		Spec struct {
			Replicas int32 `json:"replicas"`
		} `json:"spec"`
		Status struct {
			Replicas          int32 `json:"replicas"`
			ReadyReplicas     int32 `json:"readyReplicas"`
			AvailableReplicas int32 `json:"availableReplicas"`
		} `json:"status"`
	}
	if err := c.get("/apis/apps/v1/namespaces/"+ns+"/deployments/"+name, &d); err != nil {
		return false, err
	}
	return d.Spec.Replicas == 0 && d.Status.Replicas == 0 && d.Status.ReadyReplicas == 0, nil
}

func (c *RealClient) WaitReady(ns, name string, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 30 * time.Minute
	}
	deadline := time.Now().Add(timeout)
	for {
		var d struct {
			Status struct {
				ReadyReplicas     int32 `json:"readyReplicas"`
				AvailableReplicas int32 `json:"availableReplicas"`
			} `json:"status"`
		}
		if err := c.get("/apis/apps/v1/namespaces/"+ns+"/deployments/"+name, &d); err != nil {
			return err
		}
		if d.Status.ReadyReplicas >= 1 || d.Status.AvailableReplicas >= 1 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for deployment %s/%s to be ready", ns, name)
		}
		time.Sleep(3 * time.Second)
	}
}

func (c *RealClient) Health(hostIP string, port int, path string, timeout time.Duration) (bool, error) {
	if path == "" {
		path = "/health"
	}
	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(fmt.Sprintf("http://%s:%d%s", hostIP, port, path))
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode == http.StatusOK, nil
}

func (c *RealClient) SetConfigMap(name string, data map[string]string) error {
	ns := c.Namespace
	if ns == "" {
		ns = "llm-test"
	}
	return c.do("PATCH", c.base+"/api/v1/namespaces/"+ns+"/configmaps/"+name, map[string]any{"data": data})
}

func (c *RealClient) get(path string, into any) error {
	req, err := http.NewRequest(http.MethodGet, c.base+path, nil)
	if err != nil {
		return err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("not found: %s", path)
	}
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("k8s GET %s: %d %s", path, resp.StatusCode, string(b))
	}
	return json.NewDecoder(resp.Body).Decode(into)
}

func (c *RealClient) do(method, url string, body any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		return err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/merge-patch+json")
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("k8s %s %s: %d %s", method, url, resp.StatusCode, string(b))
	}
	return nil
}
