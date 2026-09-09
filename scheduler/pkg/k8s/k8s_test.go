package k8s

import (
	"sync"
	"testing"
	"time"
)

// fakeClient records call order and simulates deployment/health states.
type fakeClient struct {
	mu          sync.Mutex
	callOrder   []string
	scaled      map[string]int32
	gone        func(name string) bool
	ready       func(name string) bool
	healthUp    bool
	healthCalls int
	healthGate  chan struct{} // when set, Health blocks until closed
	firstHealth chan struct{} // closed on first Health call
	firstClosed bool
}

func newFake() *fakeClient {
	return &fakeClient{
		scaled:   map[string]int32{},
		gone:     func(string) bool { return true },
		ready:    func(string) bool { return true },
		healthUp: true,
	}
}

func (f *fakeClient) ScaleDeployment(ns, name string, r int32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scaled[name] = r
	f.callOrder = append(f.callOrder, "scale:"+name)
	return nil
}

func (f *fakeClient) DeploymentGone(ns, name string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gone(name), nil
}

func (f *fakeClient) WaitReady(ns, name string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.callOrder = append(f.callOrder, "ready:"+name)
	if !f.ready(name) {
		return &readyErr{name: name}
	}
	return nil
}

func (f *fakeClient) Health(_ string, _ int, _ string, _ time.Duration) (bool, error) {
	f.mu.Lock()
	f.healthCalls++
	if f.firstHealth != nil && !f.firstClosed {
		f.firstClosed = true
		close(f.firstHealth)
	}
	gate := f.healthGate
	f.mu.Unlock()
	if gate != nil {
		<-gate
	}
	return f.healthUp, nil
}

func (f *fakeClient) SetConfigMap(name string, data map[string]string) error { return nil }

type readyErr struct{ name string }

func (e *readyErr) Error() string { return "not ready: " + e.name }

func TestSwapOrderWorkerThenHead(t *testing.T) {
	f := newFake()
	sw := NewSwapper(f)
	from := &Target{WorkerDeploy: "old-worker", HeadDeploy: "old-head", Namespace: "llm-test"}
	to := &Target{WorkerDeploy: "qwen-worker", HeadDeploy: "qwen-head", Namespace: "llm-test",
		HostIP: "172.16.4.55", Port: 8888, HealthPath: "/health", ReadyTimeout: time.Minute}
	if err := sw.Swap(from, to); err != nil {
		t.Fatalf("swap failed: %v", err)
	}
	want := []string{
		"scale:old-worker", "scale:old-head",
		"scale:qwen-worker", "ready:qwen-worker", "scale:qwen-head",
	}
	if len(f.callOrder) != len(want) {
		t.Fatalf("unexpected call order: %v", f.callOrder)
	}
	for i := range want {
		if f.callOrder[i] != want[i] {
			t.Fatalf("call[%d]=%q want %q (all=%v)", i, f.callOrder[i], want[i], f.callOrder)
		}
	}
	if f.healthCalls == 0 {
		t.Fatalf("expected head /health poll")
	}
}

func TestSwapSingleFlight(t *testing.T) {
	f := newFake()
	f.healthGate = make(chan struct{})
	f.firstHealth = make(chan struct{})
	sw := NewSwapper(f)
	to := &Target{WorkerDeploy: "w", HeadDeploy: "h", Namespace: "ns", HostIP: "x", Port: 1, ReadyTimeout: time.Minute}

	done := make(chan struct{})
	go func() {
		_ = sw.Swap(nil, to)
		close(done)
	}()

	// Wait until the first swap is holding the lock (blocked in /health).
	<-f.firstHealth

	if err := sw.Swap(nil, to); err != ErrBusy {
		t.Fatalf("expected ErrBusy while swap in progress, got %v", err)
	}

	close(f.healthGate)
	<-done
}

func TestSwapRespectsReadyFailure(t *testing.T) {
	f := newFake()
	f.ready = func(name string) bool { return name != "bad-worker" }
	sw := NewSwapper(f)
	to := &Target{WorkerDeploy: "bad-worker", HeadDeploy: "h", Namespace: "ns", HostIP: "x", Port: 1}
	if err := sw.Swap(nil, to); err == nil {
		t.Fatalf("expected ready failure, got nil")
	}
}
