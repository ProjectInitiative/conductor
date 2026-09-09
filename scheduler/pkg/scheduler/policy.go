// Package scheduler is the policy engine behind Conductor. It decides, for a
// single GPU residency slot shared by several swappable LLM models, which model
// should be resident and whether an incoming request may be served now (green),
// queued, or triggers a model load.
//
// It is deliberately pure and clock-injectable so the FIFO / priority /
// traffic-light / anti-thrash behavior can be fully unit-tested without a
// cluster.
package scheduler

import "time"

// Clock abstracts time so tests can drive simulated time.
type Clock interface {
	Now() time.Time
}

type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

// Model describes one swappable model's scheduling params.
type Model struct {
	Name        string
	Priority    int           // lower = higher priority (1 is highest)
	MaxRequests int           // traffic-light quantum: max requests served per turn
	MaxSeconds  int           // traffic-light quantum: max seconds served per turn
	MinDwell    time.Duration // stay resident at least this long once loaded
	Cooldown    time.Duration // min time before switching back after a turn
}

// RequestOutcome is the gateway's decision for an incoming request.
type RequestOutcome int

const (
	// Serve: model is resident and its window is green — forward now.
	Serve RequestOutcome = iota
	// Park: model is resident but the light is red, or not resident — park it.
	Park
	// Load: model is not resident and should be loaded soon.
	Load
)

// Policy is the scheduling state machine. Safe for a single goroutine owner.
type Policy struct {
	clock Clock

	models map[string]*Model
	order  []string // model names sorted by priority (for next-model selection)

	// resident state
	resident  string
	loadedAt  time.Time
	activeReq int

	// per-model FIFO queues
	queues map[string]*Queue

	// turn bookkeeping for anti-thrash
	lastTurn map[string]time.Time
}

// NewPolicy builds a Policy from models.
func NewPolicy(clock Clock, models []Model) *Policy {
	p := &Policy{
		clock:    clock,
		models:   map[string]*Model{},
		queues:   map[string]*Queue{},
		lastTurn: map[string]time.Time{},
	}
	for _, m := range models {
		mm := m
		p.models[m.Name] = &mm
		p.queues[m.Name] = NewQueue()
		p.order = append(p.order, m.Name)
	}
	// stable sort by priority ascending (highest priority = lowest number)
	sortModels(p.order, p.models)
	return p
}

func (p *Policy) queue(name string) *Queue {
	if q, ok := p.queues[name]; ok {
		return q
	}
	q := NewQueue()
	p.queues[name] = q
	return q
}

// Enqueue adds a request to a model's FIFO queue.
func (p *Policy) Enqueue(model, reqID string) {
	p.queue(model).Push(reqID)
}

// Len returns the queued request count for a model.
func (p *Policy) Len(model string) int {
	return p.queue(model).Len()
}

// Resident returns the current resident model ("" if none).
func (p *Policy) Resident() string { return p.resident }

// Names returns the model names in priority order (a copy, safe to iterate).
func (p *Policy) Names() []string {
	out := make([]string, len(p.order))
	copy(out, p.order)
	return out
}

// Model returns the scheduling params for a model, or nil if unknown.
func (p *Policy) Model(name string) *Model {
	if m, ok := p.models[name]; ok {
		return m
	}
	return nil
}

// Admit decides the outcome for an incoming request for model at time now.
func (p *Policy) Admit(model string) RequestOutcome {
	now := p.clock.Now()
	if p.resident == "" {
		return Load
	}
	if p.resident != model {
		return Park
	}
	// Resident model.
	if p.green(now) {
		return Serve
	}
	return Park
}

// green reports whether the resident is still inside its traffic-light window.
func (p *Policy) green(now time.Time) bool {
	m := p.models[p.resident]
	if m == nil {
		return false
	}
	if m.MaxRequests > 0 && p.activeReq >= m.MaxRequests {
		return false
	}
	if m.MaxSeconds > 0 && now.Sub(p.loadedAt) >= time.Duration(m.MaxSeconds)*time.Second {
		return false
	}
	return true
}

// Tick advances the resident's active-request count for one served request.
func (p *Policy) Tick() { p.activeReq++ }

// NextModel returns the model to load next, or "" if nothing needs loading.
// It picks the highest-priority model with a non-empty queue, respecting
// min_dwell and cooldown of the current resident.
func (p *Policy) NextModel() string {
	now := p.clock.Now()

	// Prefer the resident while it still has queued work and hasn't hit its
	// traffic-light cap (stickiness / anti-thrash) — but only if we're allowed
	// to keep serving. We always return a model to load only when the resident
	// is NOT the one we'd serve next, to avoid needless swaps.
	if p.resident != "" {
		// If the resident is green and has queued work, don't switch.
		if p.green(now) && p.Len(p.resident) > 0 {
			return ""
		}
		// Respect min dwell: don't evict a freshly loaded resident.
		if now.Sub(p.loadedAt) < p.models[p.resident].MinDwell {
			return ""
		}
	}

	// Suggest the highest-priority model with queued work that isn't subject to
	// a cooldown.
	best := ""
	for _, name := range p.order {
		if p.Len(name) == 0 {
			continue
		}
		if name == p.resident {
			continue
		}
		if c := p.models[name].Cooldown; c > 0 {
			if last, ok := p.lastTurn[name]; ok && now.Sub(last) < c {
				continue
			}
		}
		best = name
		break
	}
	return best
}

// StartTurn records that a model became resident and resets its window.
func (p *Policy) StartTurn(model string) {
	p.resident = model
	p.loadedAt = p.clock.Now()
	p.activeReq = 0
}

// EndTurn clears residency and starts the model's cooldown clock (anti-thrash:
// a just-finished model can't be re-loaded until its cooldown expires).
func (p *Policy) EndTurn() {
	if p.resident != "" {
		p.lastTurn[p.resident] = p.clock.Now()
	}
	p.resident = ""
	p.activeReq = 0
}
