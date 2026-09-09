package scheduler

import (
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (f *fakeClock) Now() time.Time          { return f.t }
func (f *fakeClock) advance(d time.Duration) { f.t = f.t.Add(d) }

func testModels() []Model {
	return []Model{
		{Name: "deepseek", Priority: 1, MaxRequests: 10, MaxSeconds: 600, MinDwell: time.Minute, Cooldown: time.Minute},
		{Name: "qwen", Priority: 2, MaxRequests: 8, MaxSeconds: 600, MinDwell: time.Minute, Cooldown: time.Minute},
		{Name: "glm", Priority: 3, MaxRequests: 8, MaxSeconds: 600, MinDwell: time.Minute, Cooldown: time.Minute},
	}
}

func TestPriorityOrder(t *testing.T) {
	clock := &fakeClock{t: time.Now()}
	p := NewPolicy(clock, testModels())
	if p.order[0] != "deepseek" || p.order[1] != "qwen" || p.order[2] != "glm" {
		t.Fatalf("expected deepseek > qwen > glm priority order, got %v", p.order)
	}
}

func TestQueueFIFO(t *testing.T) {
	q := NewQueue()
	q.Push("a")
	q.Push("b")
	q.Push("c")
	for i, want := range []string{"a", "b", "c"} {
		got, ok := q.Pop()
		if !ok || got != want {
			t.Fatalf("pop %d: got %q ok=%v want %q", i, got, ok, want)
		}
	}
	if _, ok := q.Pop(); ok {
		t.Fatal("expected empty")
	}
}

func TestAdmitWhenNotResidentLoads(t *testing.T) {
	clock := &fakeClock{t: time.Now()}
	p := NewPolicy(clock, testModels())
	p.Enqueue("deepseek", "r1")
	if got := p.Admit("deepseek"); got != Load {
		t.Fatalf("expected Load for non-resident, got %v", got)
	}
}

func TestAdmitResidentGreenServes(t *testing.T) {
	clock := &fakeClock{t: time.Now()}
	p := NewPolicy(clock, testModels())
	p.StartTurn("deepseek")
	if got := p.Admit("deepseek"); got != Serve {
		t.Fatalf("expected Serve for resident green, got %v", got)
	}
}

func TestTrafficLightCutsOff(t *testing.T) {
	clock := &fakeClock{t: time.Now()}
	models := []Model{{Name: "deepseek", Priority: 1, MaxRequests: 2, MinDwell: 0, Cooldown: 0}}
	p := NewPolicy(clock, models)
	p.StartTurn("deepseek")
	p.Tick()
	p.Tick()
	if got := p.Admit("deepseek"); got != Park {
		t.Fatalf("expected Park after quantum reached, got %v", got)
	}
}

func TestNextModelPicksHighestPriority(t *testing.T) {
	clock := &fakeClock{t: time.Now()}
	p := NewPolicy(clock, testModels())
	p.StartTurn("deepseek")
	p.EndTurn() // resident cleared
	p.Enqueue("glm", "g1")
	p.Enqueue("qwen", "q1")
	if got := p.NextModel(); got != "qwen" {
		t.Fatalf("expected qwen (priority 2), got %q", got)
	}
}

func TestStickinessKeepsResidentWithWork(t *testing.T) {
	clock := &fakeClock{t: time.Now()}
	p := NewPolicy(clock, testModels())
	p.StartTurn("deepseek")
	p.Enqueue("deepseek", "d1")
	p.Enqueue("glm", "g1")
	// Resident is green and has work -> don't switch.
	if got := p.NextModel(); got != "" {
		t.Fatalf("expected no switch (stickiness), got %q", got)
	}
}

func TestMinDwellPreventsRapidEvict(t *testing.T) {
	clock := &fakeClock{t: time.Now()}
	p := NewPolicy(clock, testModels())
	p.StartTurn("deepseek")
	p.EndTurn() // simulate draining; resident still has dwell clock
	// Re-load a different model right away should not be allowed before dwell.
	p.StartTurn("deepseek")
	p.Enqueue("glm", "g1")
	if got := p.NextModel(); got != "" {
		t.Fatalf("expected no switch within min dwell, got %q", got)
	}
}

func TestCooldownPreventsFlipFlop(t *testing.T) {
	clock := &fakeClock{t: time.Now()}
	models := []Model{
		{Name: "a", Priority: 1, MaxRequests: 10, MinDwell: 0, Cooldown: 5 * time.Minute},
		{Name: "b", Priority: 2, MaxRequests: 10, MinDwell: 0, Cooldown: 5 * time.Minute},
	}
	p := NewPolicy(clock, models)
	p.StartTurn("a")
	p.EndTurn() // a just finished its turn -> cooldown starts
	p.StartTurn("b")
	p.Enqueue("a", "d1")
	// a is inside its cooldown -> must not flip back to a, and nothing else is
	// loadable, so no switch.
	if got := p.NextModel(); got != "" {
		t.Fatalf("expected cooldown to block a, got %q", got)
	}
	clock.advance(6 * time.Minute)
	if got := p.NextModel(); got != "a" {
		t.Fatalf("expected a after cooldown, got %q", got)
	}
}
