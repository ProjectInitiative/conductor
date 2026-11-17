package controller

import (
	"sort"
	"strconv"

	corev1 "k8s.io/api/core/v1"
)

// Placement decides which nodes a DeploymentSet pair lands on. On a
// switchless fabric ring, a tensor-parallel pair wants ring-adjacent nodes:
// every ring edge is a direct RoCE rail pair, while non-adjacent pairs fall
// back to routed RoCE through a ring neighbour (correct, half the striped
// bandwidth). See keel OQ-0055.

// Placement modes for PlacementSpec.Mode.
const (
	// PlacementModeBestRoute prefers a ring-adjacent pair (direct rails) and
	// places freely when none is Ready — routed RoCE serves the fallback.
	PlacementModeBestRoute = "bestRoute"
	// PlacementModeAdjacent requires a ring-adjacent pair and blocks
	// (NoAdjacentPair) rather than degrading to a routed hop.
	PlacementModeAdjacent = "adjacent"
	// PlacementModeAny never pins; the scheduler decides. Useful to exercise
	// the routed path deliberately.
	PlacementModeAny = "any"
)

// DefaultRingLabel is the node label carrying the ring walking order, set by
// the dgx-spark host module: node[i].P0 cables to node[i+1].P1, and the last
// node's P0 wraps to node[0]'s P1.
const DefaultRingLabel = "fabric.homelab.io/ring-position"

// ringNode is one node of the fabric ring.
type ringNode struct {
	Name     string
	Position int
	Ready    bool
}

// adjacentPair is one direct fabric edge. Orientation only needs to be
// deterministic — the TP2 head/worker roles are symmetric, so the worker sits
// at position i and the head at position i+1.
type adjacentPair struct {
	Worker ringNode
	Head   ringNode
}

// nodeReady reports placement eligibility: NodeReady condition True and not
// cordoned (spec.unschedulable). Pool taints are deliberately ignored — every
// member of a dedicated pool tolerates its own taints (the dgx-spark nodes
// carry dgx-spark-dedicated=true:NoSchedule and the serving lanes tolerate
// it), so taint-awareness here would collapse placement on dedicated pools.
// Quarantine uses cordon, which both the scheduler and placement skip.
func nodeReady(node *corev1.Node) bool {
	if node.Spec.Unschedulable {
		return false
	}
	for i := range node.Status.Conditions {
		c := &node.Status.Conditions[i]
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// ringFromNodes returns ring-labelled nodes in ring walking order. Nodes
// without the label, with an unparsable position, or not Ready are skipped;
// duplicate positions keep the first-seen node.
func ringFromNodes(nodes []corev1.Node, ringLabel string) []ringNode {
	seen := map[int]bool{}
	ring := make([]ringNode, 0, len(nodes))
	for i := range nodes {
		node := &nodes[i]
		raw, ok := node.Labels[ringLabel]
		if !ok {
			continue
		}
		pos, err := strconv.Atoi(raw)
		if err != nil {
			continue
		}
		if seen[pos] {
			continue
		}
		seen[pos] = true
		ring = append(ring, ringNode{Name: node.Name, Position: pos, Ready: nodeReady(node)})
	}
	sort.Slice(ring, func(i, j int) bool { return ring[i].Position < ring[j].Position })
	return ring
}

// chooseAdjacentPair returns the first adjacent ring pair whose two nodes are
// both Ready, walking the ring from position 0 (deterministic). It returns
// nil when no edge is fully ready or the ring has fewer than two members —
// the caller then places freely, which routed RoCE serves on non-adjacent
// pairs.
func chooseAdjacentPair(nodes []corev1.Node, ringLabel string) *adjacentPair {
	ring := ringFromNodes(nodes, ringLabel)
	if len(ring) < 2 {
		return nil
	}
	for i := 0; i < len(ring); i++ {
		worker := ring[i]
		head := ring[(i+1)%len(ring)]
		if worker.Ready && head.Ready {
			return &adjacentPair{Worker: worker, Head: head}
		}
	}
	return nil
}
