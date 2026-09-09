package scheduler

import "sync"

// Engine is the concurrency-safe facade over a Policy. The Policy itself is a
// single-owner state machine; Engine guards it with a RWMutex so the HTTP
// gateway and Fission functions can call it concurrently. It also maps each
// model to its resident backend URL.
type Engine struct {
	mu       sync.RWMutex
	policy   *Policy
	backends map[string]string // model -> backend base URL (e.g. http://172.16.4.55:8000)
}

// NewEngine builds an Engine. models defines the swappable models (with their
// scheduling params) and backends maps each model to its vLLM base URL.
func NewEngine(models []Model, backends map[string]string) *Engine {
	if backends == nil {
		backends = map[string]string{}
	}
	return &Engine{
		policy:   NewPolicy(RealClock{}, models),
		backends: backends,
	}
}

// Admit decides whether a request for model may be served now.
func (e *Engine) Admit(model string) RequestOutcome {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.policy.Admit(model)
}

// Tick records that the resident served one request (advances the traffic-light
// quantum counter).
func (e *Engine) Tick() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.policy.Tick()
}

// Enqueue parks a request id on a model's FIFO queue.
func (e *Engine) Enqueue(model, id string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.policy.Enqueue(model, id)
}

// NextModel returns the model to load next ("" if none).
func (e *Engine) NextModel() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.policy.NextModel()
}

// StartTurn marks a model resident.
func (e *Engine) StartTurn(model string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.policy.StartTurn(model)
}

// EndTurn clears residency.
func (e *Engine) EndTurn() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.policy.EndTurn()
}

// Len returns the queued request count for a model.
func (e *Engine) Len(model string) int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.policy.Len(model)
}

// Resident returns the current resident model ("" if none).
func (e *Engine) Resident() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.policy.Resident()
}

// Backend returns the backend base URL for a model.
func (e *Engine) Backend(model string) (string, bool) {
	url, ok := e.backends[model]
	return url, ok
}

// ModelStatus is the per-model view exposed by Status.
type ModelStatus struct {
	Queued   int  `json:"queued"`
	Priority int  `json:"priority"`
	Resident bool `json:"resident"`
}

// Status is a point-in-time snapshot of the scheduler.
type Status struct {
	Resident string                 `json:"resident"`
	Models   map[string]ModelStatus `json:"models"`
}

// Status returns a snapshot of resident + per-model queue/priority info.
func (e *Engine) Status() Status {
	e.mu.RLock()
	defer e.mu.RUnlock()
	st := Status{Resident: e.policy.Resident(), Models: map[string]ModelStatus{}}
	names := e.policy.Names()
	for _, name := range names {
		m := e.policy.Model(name)
		if m == nil {
			continue
		}
		st.Models[name] = ModelStatus{
			Queued:   e.policy.Len(name),
			Priority: m.Priority,
			Resident: name == st.Resident,
		}
	}
	return st
}
