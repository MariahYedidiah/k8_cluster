package clustercriticality

import (
	"context"
	"fmt"
	"math"

	v1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const hybridPreScoreStateKey = "PreScoreHybridMerged"

type hybridPreScoreState struct {
	podCPU            int64
	podMemory         int64
	podStorage        int64
	topologyScores    map[string]int64
	taintScores       map[string]int64
	clusterScores     map[string]int64
	podCluster        map[string]int
	clusterNodeScores map[int]map[string]int64
}

func (s *hybridPreScoreState) Clone() fwk.StateData {
	return s
}

func (h *HybridScore) PreScore(
	ctx context.Context,
	state fwk.CycleState,
	pod *v1.Pod,
	nodes []fwk.NodeInfo,
) *fwk.Status {
	ps := &hybridPreScoreState{
		podCPU:            podCPURequest(pod),
		podMemory:         podMemoryRequest(pod),
		podStorage:        podStorageRequest(pod),
		taintScores:       map[string]int64{},
		topologyScores:    map[string]int64{},
		clusterScores:     map[string]int64{},
		podCluster:        map[string]int{},
		clusterNodeScores: map[int]map[string]int64{},
	}

	// -------------------------
	// 1. Taint scoring
	// -------------------------
	rawTaintCounts := map[string]int64{}

	var minRaw int64 = math.MaxInt64
	var maxRaw int64 = math.MinInt64

	for _, nodeInfo := range nodes {
		node := nodeInfo.Node()
		if node == nil {
			continue
		}

		raw := countUntoleratedPreferNoScheduleTaints(pod, node)
		rawTaintCounts[node.Name] = raw

		if raw < minRaw {
			minRaw = raw
		}
		if raw > maxRaw {
			maxRaw = raw
		}
	}

	for nodeName, raw := range rawTaintCounts {
		if maxRaw == minRaw {
			ps.taintScores[nodeName] = framework.MaxNodeScore
			continue
		}

		score := (maxRaw - raw) * framework.MaxNodeScore / (maxRaw - minRaw)

		if score < 0 {
			score = 0
		}
		if score > framework.MaxNodeScore {
			score = framework.MaxNodeScore
		}

		ps.taintScores[nodeName] = score
	}

	// -------------------------
	// 2. Zone topology spread scoring
	// Active only if pod YAML mentions topology.kubernetes.io/zone
	// -------------------------

	zoneTopologyKey := "topology.kubernetes.io/zone"
	hasZoneTopologyConstraint := false

	for _, c := range pod.Spec.TopologySpreadConstraints {
		if c.TopologyKey == zoneTopologyKey {
			hasZoneTopologyConstraint = true
			break
		}
	}

	if hasZoneTopologyConstraint {
		zoneCounts := map[string]int64{}

		appLabel := pod.Labels["app"]

		for _, nodeInfo := range nodes {
			node := nodeInfo.Node()
			if node == nil {
				continue
			}

			zone := node.Labels[zoneTopologyKey]
			if zone == "" {
				continue
			}

			if _, ok := zoneCounts[zone]; !ok {
				zoneCounts[zone] = 0
			}

			if appLabel != "" {
				for _, p := range nodeInfo.GetPods() {
					existingPod := p.GetPod()
					if existingPod != nil && existingPod.Labels["app"] == appLabel {
						zoneCounts[zone]++
					}
				}
			}
		}

		var minCount int64 = math.MaxInt64
		var maxCount int64 = math.MinInt64

		for _, count := range zoneCounts {
			if count < minCount {
				minCount = count
			}
			if count > maxCount {
				maxCount = count
			}
		}

		for _, nodeInfo := range nodes {
			node := nodeInfo.Node()
			if node == nil {
				continue
			}

			zone := node.Labels[zoneTopologyKey]
			if zone == "" {
				ps.topologyScores[node.Name] = 0
				continue
			}

			count := zoneCounts[zone]

			if maxCount == minCount {
				ps.topologyScores[node.Name] = framework.MaxNodeScore
				continue
			}

			// Fewer same-app pods in this zone = better spread.
			score := (maxCount - count) * framework.MaxNodeScore / (maxCount - minCount)

			if score < 0 {
				score = 0
			}
			if score > framework.MaxNodeScore {
				score = framework.MaxNodeScore
			}

			ps.topologyScores[node.Name] = score
		}
	}
	ps.clusterScores, ps.podCluster, ps.clusterNodeScores =
		h.computeFullClusterScores(pod, nodes)
	state.Write(hybridPreScoreStateKey, ps)
	return nil
}

func getHybridPreScoreState(state fwk.CycleState) (*hybridPreScoreState, error) {
	c, err := state.Read(hybridPreScoreStateKey)
	if err != nil {
		return nil, fmt.Errorf("reading %s from CycleState: %w", hybridPreScoreStateKey, err)
	}

	s, ok := c.(*hybridPreScoreState)
	if !ok {
		return nil, fmt.Errorf("invalid HybridMerged PreScore state")
	}

	return s, nil
}

func countUntoleratedPreferNoScheduleTaints(pod *v1.Pod, node *v1.Node) int64 {
	var count int64

	for _, taint := range node.Spec.Taints {
		if taint.Effect != v1.TaintEffectPreferNoSchedule {
			continue
		}

		tolerated := false

		for _, tol := range pod.Spec.Tolerations {
			if tol.ToleratesTaint(&taint) {
				tolerated = true
				break
			}
		}

		if !tolerated {
			count++
		}
	}

	return count
}
