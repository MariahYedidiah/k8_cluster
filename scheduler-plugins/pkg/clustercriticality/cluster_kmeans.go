package clustercriticality

import (
	"math"
	"sort"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fwk "k8s.io/kube-scheduler/framework"
)

type clusterPod struct {
	key       string
	app       string
	image     string
	layers    []string
	tolerates int
	nodePrefs map[string]float64
	podPrefs  map[string]float64
	antiApps  map[string]bool
	topology  bool
}

type clusterModel struct {
	images     []string
	layers     []string
	apps       []string
	nodeLabels []string

	vectors   [][]float64
	centroids [][]float64
	labels    []int

	clusterSizes    []int
	clusterApps     []map[string]bool
	clusterTopology []map[string]int

	podCluster       map[string]int
	clusterNodeScore map[int]map[string]int64
}

func (h *HybridScore) computeFullClusterScores(pod *v1.Pod, nodes []fwk.NodeInfo) (map[string]int64, map[string]int, map[int]map[string]int64) {
	allPods := h.collectClusterPods(pod, nodes)

	if len(allPods) == 0 {
		return map[string]int64{}, map[string]int{}, map[int]map[string]int64{}
	}

	model := &clusterModel{}
	model.buildVocab(allPods, nodes)
	model.buildPodVectors(allPods)

	k := int(math.Ceil(math.Sqrt(float64(len(allPods)))))
	if k < 1 {
		k = 1
	}
	if k > len(allPods) {
		k = len(allPods)
	}

	model.runKMeans(k, len(allPods))
	model.computeClusterMetadata(allPods, k)
	model.refineClusters(allPods, k, 2)
	model.recomputeCentroids(k)
	model.computeClusterMetadata(allPods, k)
	model.assignPodClusters(allPods)
	model.precomputeClusterNodeScores(h, nodes)

	pendingKey := clusterPodKey(pod)
	clusterID, ok := model.podCluster[pendingKey]
	if !ok {
		clusterID = 0
	}

	nodeScores := map[string]int64{}

	for _, nodeInfo := range nodes {
		node := nodeInfo.Node()
		if node == nil {
			continue
		}

		score := int64(0)
		if byNode, ok := model.clusterNodeScore[clusterID]; ok {
			if v, ok := byNode[node.Name]; ok {
				score = v
			}
		}

		nodeScores[node.Name] = score
	}

	return nodeScores, model.podCluster, model.clusterNodeScore
}

func (h *HybridScore) collectClusterPods(pending *v1.Pod, nodes []fwk.NodeInfo) []clusterPod {
	out := []clusterPod{}

	for _, nodeInfo := range nodes {
		for _, pi := range nodeInfo.GetPods() {
			p := pi.GetPod()
			if p == nil {
				continue
			}
			out = append(out, h.toClusterPod(p))
		}
	}

	out = append(out, h.toClusterPod(pending))
	return out
}

func (h *HybridScore) toClusterPod(pod *v1.Pod) clusterPod {
	image := ""
	layers := []string{}

	if len(pod.Spec.Containers) > 0 {
		image = pod.Spec.Containers[0].Image
	}

	h.mu.RLock()
	if meta, ok := h.images[image]; ok {
		for _, layer := range meta.Layers {
			layers = append(layers, layer.Layer)
		}
	}
	h.mu.RUnlock()

	nodePrefs := map[string]float64{}
	aff := pod.Spec.Affinity

	if aff != nil && aff.NodeAffinity != nil {
		for _, pref := range aff.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
			weight := float64(pref.Weight)
			for _, expr := range pref.Preference.MatchExpressions {
				for _, val := range expr.Values {
					nodePrefs[expr.Key+"="+val] += weight
				}
			}
		}
	}

	podPrefs := map[string]float64{}
	antiApps := map[string]bool{}

	if aff != nil && aff.PodAffinity != nil {
		for _, pref := range aff.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
			app := podAffinityApp(pref.PodAffinityTerm.LabelSelector)
			if app != "" {
				podPrefs[app] += float64(pref.Weight)
			}
		}
	}

	if aff != nil && aff.PodAntiAffinity != nil {
		for _, pref := range aff.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
			app := podAffinityApp(pref.PodAffinityTerm.LabelSelector)
			if app != "" {
				antiApps[app] = true
			}
		}
	}

	hasTopology := false
	for _, c := range pod.Spec.TopologySpreadConstraints {
		if c.TopologyKey == "topology.kubernetes.io/zone" {
			hasTopology = true
			break
		}
	}

	return clusterPod{
		key:       clusterPodKey(pod),
		app:       pod.Labels["app"],
		image:     image,
		layers:    layers,
		tolerates: len(pod.Spec.Tolerations),
		nodePrefs: nodePrefs,
		podPrefs:  podPrefs,
		antiApps:  antiApps,
		topology:  hasTopology,
	}
}

func clusterPodKey(pod *v1.Pod) string {
	return pod.Namespace + "/" + pod.Name
}

func podAffinityApp(sel *metav1.LabelSelector) string {
	if sel == nil {
		return ""
	}
	if v, ok := sel.MatchLabels["app"]; ok {
		return v
	}
	return ""
}

func (m *clusterModel) buildVocab(pods []clusterPod, nodes []fwk.NodeInfo) {
	imageSet := map[string]bool{}
	layerSet := map[string]bool{}
	appSet := map[string]bool{}
	labelSet := map[string]bool{}

	for _, p := range pods {
		if p.image != "" {
			imageSet[p.image] = true
		}
		if p.app != "" {
			appSet[p.app] = true
		}
		for _, l := range p.layers {
			layerSet[l] = true
		}
		for k := range p.nodePrefs {
			labelSet[k] = true
		}
		for a := range p.podPrefs {
			appSet[a] = true
		}
		for a := range p.antiApps {
			appSet[a] = true
		}
	}

	for _, ni := range nodes {
		node := ni.Node()
		if node == nil {
			continue
		}

		for k, v := range node.Labels {
			labelSet[k+"="+v] = true
		}

		for _, pi := range ni.GetPods() {
			p := pi.GetPod()
			if p == nil {
				continue
			}
			if len(p.Spec.Containers) > 0 {
				imageSet[p.Spec.Containers[0].Image] = true
			}
			if app := p.Labels["app"]; app != "" {
				appSet[app] = true
			}
		}
	}

	m.images = sortedKeys(imageSet)
	m.layers = sortedKeys(layerSet)
	m.apps = sortedKeys(appSet)
	m.nodeLabels = sortedKeys(labelSet)
}

func sortedKeys(set map[string]bool) []string {
	keys := []string{}
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (m *clusterModel) buildPodVectors(pods []clusterPod) {
	m.vectors = [][]float64{}

	for _, p := range pods {
		vec := []float64{}

		for _, image := range m.images {
			if p.image == image {
				vec = append(vec, 1.0)
			} else {
				vec = append(vec, 0.0)
			}
		}

		layerMap := map[string]bool{}
		for _, l := range p.layers {
			layerMap[l] = true
		}

		for _, layer := range m.layers {
			if layerMap[layer] {
				vec = append(vec, 1.0)
			} else {
				vec = append(vec, 0.0)
			}
		}

		tol := float64(p.tolerates) / 5.0
		if tol > 1 {
			tol = 1
		}
		vec = append(vec, tol)

		maxPref := 1.0
		for _, v := range p.nodePrefs {
			if v > maxPref {
				maxPref = v
			}
		}

		for _, label := range m.nodeLabels {
			vec = append(vec, p.nodePrefs[label]/maxPref)
		}

		maxPodPref := 1.0
		for _, v := range p.podPrefs {
			if v > maxPodPref {
				maxPodPref = v
			}
		}

		for _, app := range m.apps {
			vec = append(vec, p.podPrefs[app]/maxPodPref)
		}

		m.vectors = append(m.vectors, vec)
	}
}

func (m *clusterModel) runKMeans(k int, maxIters int) {
	n := len(m.vectors)
	d := len(m.vectors[0])

	m.centroids = make([][]float64, k)
	for c := 0; c < k; c++ {
		idx := c % n
		m.centroids[c] = append([]float64{}, m.vectors[idx]...)
	}

	m.labels = make([]int, n)

	for iter := 0; iter < maxIters; iter++ {
		changed := false

		for i := 0; i < n; i++ {
			best := 0
			bestDist := distance(m.vectors[i], m.centroids[0])

			for c := 1; c < k; c++ {
				dist := distance(m.vectors[i], m.centroids[c])
				if dist < bestDist {
					bestDist = dist
					best = c
				}
			}

			if m.labels[i] != best {
				m.labels[i] = best
				changed = true
			}
		}

		newCentroids := make([][]float64, k)
		counts := make([]int, k)

		for c := 0; c < k; c++ {
			newCentroids[c] = make([]float64, d)
		}

		for i := 0; i < n; i++ {
			c := m.labels[i]
			counts[c]++
			for j := 0; j < d; j++ {
				newCentroids[c][j] += m.vectors[i][j]
			}
		}

		for c := 0; c < k; c++ {
			if counts[c] == 0 {
				newCentroids[c] = append([]float64{}, m.centroids[c]...)
				continue
			}
			for j := 0; j < d; j++ {
				newCentroids[c][j] /= float64(counts[c])
			}
		}

		m.centroids = newCentroids

		if !changed {
			break
		}
	}
}

func distance(a, b []float64) float64 {
	var s float64
	for i := range a {
		x := a[i] - b[i]
		s += x * x
	}
	return math.Sqrt(s)
}

func (m *clusterModel) computeClusterMetadata(pods []clusterPod, k int) {
	m.clusterSizes = make([]int, k)
	m.clusterApps = make([]map[string]bool, k)
	m.clusterTopology = make([]map[string]int, k)

	for c := 0; c < k; c++ {
		m.clusterApps[c] = map[string]bool{}
		m.clusterTopology[c] = map[string]int{}
	}

	for i, p := range pods {
		c := m.labels[i]
		m.clusterSizes[c]++

		if p.app != "" {
			m.clusterApps[c][p.app] = true
		}

		if p.topology && p.app != "" {
			m.clusterTopology[c][p.app]++
		}
	}
}

func (m *clusterModel) refineClusters(pods []clusterPod, k int, rounds int) {
	for r := 0; r < rounds; r++ {
		moved := false
		m.computeClusterMetadata(pods, k)

		for i, p := range pods {
			cur := m.labels[i]
			curCost := m.constraintCost(p, cur)

			if curCost <= 0 {
				continue
			}

			best := cur
			bestCost := curCost

			order := m.nearestClusters(m.vectors[i])

			for _, cand := range order {
				if cand == cur {
					continue
				}

				cost := m.constraintCost(p, cand)

				if cost < bestCost {
					best = cand
					bestCost = cost
					break
				}
			}

			if best != cur {
				m.labels[i] = best
				moved = true
				m.computeClusterMetadata(pods, k)
			}
		}

		if !moved {
			break
		}
	}
}
func (m *clusterModel) constraintCost(p clusterPod, c int) int {
	cost := 0

	for anti := range p.antiApps {
		if m.clusterApps[c][anti] {
			cost += 10
		}
	}

	if p.topology && p.app != "" {
		cost += 5 * m.clusterTopology[c][p.app]
	}

	// if m.clusterSizes[c] > maxSize {
	// 	cost += 5 * (m.clusterSizes[c] - maxSize)
	// }

	// if m.clusterSizes[c] < minSize {
	// 	cost += minSize - m.clusterSizes[c]
	// }

	return cost
}

// func (m *clusterModel) sizeOKAfterMove(src, dst, minSize, maxSize int) bool {
// 	srcSize := m.clusterSizes[src] - 1
// 	dstSize := m.clusterSizes[dst] + 1

// 	if dstSize > maxSize {
// 		return false
// 	}

// 	if srcSize < 0 {
// 		return false
// 	}

// 	return true
// }

func (m *clusterModel) nearestClusters(vec []float64) []int {
	type pair struct {
		id   int
		dist float64
	}

	pairs := []pair{}
	for c := range m.centroids {
		pairs = append(pairs, pair{c, distance(vec, m.centroids[c])})
	}

	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].dist < pairs[j].dist
	})

	out := []int{}
	for _, p := range pairs {
		out = append(out, p.id)
	}
	return out
}

func (m *clusterModel) recomputeCentroids(k int) {
	if len(m.vectors) == 0 {
		return
	}

	d := len(m.vectors[0])
	centroids := make([][]float64, k)
	counts := make([]int, k)

	for c := 0; c < k; c++ {
		centroids[c] = make([]float64, d)
	}

	for i, v := range m.vectors {
		c := m.labels[i]
		counts[c]++

		for j := 0; j < d; j++ {
			centroids[c][j] += v[j]
		}
	}

	for c := 0; c < k; c++ {
		if counts[c] == 0 {
			centroids[c] = append([]float64{}, m.centroids[c]...)
			continue
		}

		for j := 0; j < d; j++ {
			centroids[c][j] /= float64(counts[c])
		}
	}

	m.centroids = centroids
}

func (m *clusterModel) assignPodClusters(pods []clusterPod) {
	m.podCluster = map[string]int{}

	for i, p := range pods {
		m.podCluster[p.key] = m.labels[i]
	}
}

func (m *clusterModel) precomputeClusterNodeScores(h *HybridScore, nodes []fwk.NodeInfo) {
	m.clusterNodeScore = map[int]map[string]int64{}

	for c, centroid := range m.centroids {
		m.clusterNodeScore[c] = map[string]int64{}

		for _, nodeInfo := range nodes {
			node := nodeInfo.Node()
			if node == nil {
				continue
			}

			nodeVec := m.nodeVector(h, nodeInfo)
			dist := distance(centroid, nodeVec)

			// 1. Distance score, sharper than old maxDist normalization.
			distanceScore := int64(100.0 / (1.0 + dist))
			if distanceScore < 0 {
				distanceScore = 0
			}
			if distanceScore > 100 {
				distanceScore = 100
			}

			// 2. App locality score.
			appScore := int64(0)
			for _, pi := range nodeInfo.GetPods() {
				p := pi.GetPod()
				if p == nil {
					continue
				}
				if app := p.Labels["app"]; app != "" {
					if m.clusterApps[c][app] {
						appScore = 100
						break
					}
				}
			}

			// 3. Taint compatibility.
			taintScore := int64(100)
			badTaints := 0
			for _, t := range node.Spec.Taints {
				if t.Effect == v1.TaintEffectPreferNoSchedule {
					badTaints++
				}
			}
			taintScore = 100 - int64(badTaints*25)
			if taintScore < 0 {
				taintScore = 0
			}

			// 4. Resource balance.
			balance := int64(100)
			cpuAlloc := nodeInfo.GetAllocatable().GetMilliCPU()
			memAlloc := nodeInfo.GetAllocatable().GetMemory()
			if cpuAlloc > 0 && memAlloc > 0 {
				cpuFrac := float64(nodeInfo.GetRequested().GetMilliCPU()) / float64(cpuAlloc)
				memFrac := float64(nodeInfo.GetRequested().GetMemory()) / float64(memAlloc)
				diff := math.Abs(cpuFrac - memFrac)
				balance = int64((1.0 - math.Min(diff, 1.0)) * 100.0)
			}

			// Final cluster score.
			score :=
				0.35*float64(distanceScore) +
					0.25*float64(appScore) +
					0.20*float64(balance) +
					0.20*float64(taintScore)

			if score < 0 {
				score = 0
			}
			if score > 100 {
				score = 100
			}

			m.clusterNodeScore[c][node.Name] = int64(score)
		}
	}
}
func (m *clusterModel) nodeVector(h *HybridScore, nodeInfo fwk.NodeInfo) []float64 {
	node := nodeInfo.Node()
	vec := []float64{}

	nodeImages := map[string]bool{}
	nodeApps := map[string]bool{}

	for _, pi := range nodeInfo.GetPods() {
		p := pi.GetPod()
		if p == nil {
			continue
		}

		if len(p.Spec.Containers) > 0 {
			nodeImages[p.Spec.Containers[0].Image] = true
		}

		if app := p.Labels["app"]; app != "" {
			nodeApps[app] = true
		}
	}

	for _, image := range m.images {
		if nodeImages[image] {
			vec = append(vec, 1.0)
		} else {
			vec = append(vec, 0.0)
		}
	}

	nodeLayerSet := map[string]bool{}

	h.mu.RLock()
	for _, l := range h.nodeLayers[node.Name] {
		nodeLayerSet[l] = true
	}
	h.mu.RUnlock()

	for _, layer := range m.layers {
		if nodeLayerSet[layer] {
			vec = append(vec, 1.0)
		} else {
			vec = append(vec, 0.0)
		}
	}

	badTaints := 0
	for _, t := range node.Spec.Taints {
		if t.Effect == v1.TaintEffectPreferNoSchedule {
			badTaints++
		}
	}

	taintCompat := 1.0 - float64(badTaints)/5.0
	if taintCompat < 0 {
		taintCompat = 0
	}
	vec = append(vec, taintCompat)

	for _, label := range m.nodeLabels {
		if nodeHasLabelKeyValue(node, label) {
			vec = append(vec, 1.0)
		} else {
			vec = append(vec, 0.0)
		}
	}

	for _, app := range m.apps {
		if nodeApps[app] {
			vec = append(vec, 1.0)
		} else {
			vec = append(vec, 0.0)
		}
	}

	return vec
}

func nodeHasLabelKeyValue(node *v1.Node, kv string) bool {
	for k, v := range node.Labels {
		if k+"="+v == kv {
			return true
		}
	}
	return false
}
