package scheduler

import "sort"

// sortModels orders model names by ascending priority (highest priority first).
// Stable so ties preserve insertion order (deterministic next-model selection).
func sortModels(names []string, models map[string]*Model) {
	sort.SliceStable(names, func(i, j int) bool {
		return models[names[i]].Priority < models[names[j]].Priority
	})
}

// Queue is a simple FIFO queue.
type Queue struct {
	items []string
}

func NewQueue() *Queue { return &Queue{} }

func (q *Queue) Push(id string) { q.items = append(q.items, id) }

func (q *Queue) Pop() (string, bool) {
	if len(q.items) == 0 {
		return "", false
	}
	id := q.items[0]
	q.items = q.items[1:]
	return id, true
}

func (q *Queue) Len() int { return len(q.items) }
