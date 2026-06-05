package criticalityscore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const Name = "CriticalityScore"
const latencyNamespace = "kube-system"
const latencyConfigMap = "latency-cache"

// const imageCachePath = "/etc/lrs/cache.json"
const imageCacheNamespace = "kube-system"
const imageCacheConfigMap = "lrs-image-cache"

const nodeCacheNamespace = "kube-system"
const nodeCacheConfigMap = "lrs-node-layer-cache"

const netmarksNamespace = "kube-system"
const netmarksConfigMap = "netmarks-traffic-cache"
const runtimeNamespace = "kube-system"
const runtimeConfigMap = "runtime-node-metrics-cache"

const targetCPUUtilization = 70.0
const targetMemoryUtilization = 70.0

const (
	wLayer = 0.50
	wNet   = 0.50
)

type Layer struct {
	Layer string `json:"layer"`
	Size  int64  `json:"size"`
}
type RuntimeMetric struct {
	CPUPercent    float64 `json:"cpu_percent"`
	MemoryPercent float64 `json:"memory_percent"`
}

type RuntimeMetrics map[string]RuntimeMetric
type ImageMeta struct {
	TotalSize int64   `json:"total_size"`
	Layers    []Layer `json:"layers"`
}

type Cache struct {
	Images map[string]ImageMeta `json:"images"`
}

type TrafficData map[string]map[string]float64
type LatencyData map[string]float64

type HybridScore struct {
	handle framework.Handle

	mu sync.RWMutex

	images         map[string]ImageMeta
	nodeLayers     map[string][]string
	traffic        TrafficData
	runtimeMetrics RuntimeMetrics
	latency        LatencyData
}

var _ framework.PreScorePlugin = &HybridScore{}
var _ framework.ScorePlugin = &HybridScore{}

func New(ctx context.Context, _ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	// images, err := loadImageCache()
	// if err != nil {
	// 	return nil, err
	// }
	images := map[string]ImageMeta{}

	p := &HybridScore{
		handle:         h,
		images:         images,
		nodeLayers:     map[string][]string{},
		traffic:        TrafficData{},
		runtimeMetrics: RuntimeMetrics{},
		latency:        LatencyData{},
	}
	p.refreshImageCache(ctx)

	p.refreshRuntimeMetrics(ctx)
	p.refreshLatency(ctx)

	p.refreshNodeLayers(ctx)
	p.refreshTraffic(ctx)

	go p.refreshLoop(ctx)

	return p, nil
}

func (h *HybridScore) Name() string {
	return Name
}

func (h *HybridScore) Score(
	ctx context.Context,
	state fwk.CycleState,
	pod *v1.Pod,
	nodeInfo fwk.NodeInfo,
) (int64, *fwk.Status) {
	layer := h.layerScore(pod, nodeInfo)
	net := h.netmarksScore(pod, nodeInfo)
	latency := h.latencyScoreForNode(pod, nodeInfo)
	image := h.imageScore(pod, nodeInfo)
	ps, err := getHybridPreScoreState(state)

	if err != nil {
		ps = &hybridPreScoreState{
			podCPU:         podCPURequest(pod),
			podMemory:      podMemoryRequest(pod),
			podStorage:     podStorageRequest(pod),
			taintScores:    map[string]int64{},
			topologyScores: map[string]int64{},
		}
	}

	topology := framework.MaxNodeScore

	if v, ok := ps.topologyScores[nodeInfo.Node().Name]; ok {
		topology = v
	}
	cpu := leastAllocatedScore(
		nodeInfo.GetAllocatable().GetMilliCPU(),
		nodeInfo.GetRequested().GetMilliCPU(),
		// podCPURequest(pod),
		ps.podCPU,
	)

	mem := leastAllocatedScore(
		nodeInfo.GetAllocatable().GetMemory(),
		nodeInfo.GetRequested().GetMemory(),
		// podMemoryRequest(pod),
		ps.podMemory,
	)

	storage := leastAllocatedScore(
		nodeInfo.GetAllocatable().GetEphemeralStorage(),
		nodeInfo.GetRequested().GetEphemeralStorage(),
		// podStorageRequest(pod),
		ps.podStorage,
	)
	taint := ps.taintScores[nodeInfo.Node().Name]

	// qos := v1qos.GetPodQOS(pod)
	qos := qosScore(pod)
	nodeAffinity := nodeAffinityScore(pod, nodeInfo.Node())
	antiAffinity := podAntiAffinityScore(pod, nodeInfo)
	affinity := (nodeAffinity + antiAffinity) / 2
	cpuLoad, memLoad, runtimeLoad, predictedCPU, predictedMem :=
		h.runtimeLoadScore(pod, nodeInfo, ps)
	if taint == 0 && len(ps.taintScores) == 0 {
		taint = framework.MaxNodeScore
	}

	balance := balanceScore(nodeInfo, pod)
	weightedFinal :=
		0.08*float64(cpu) +
			0.08*float64(mem) +
			0.05*float64(storage) +
			0.15*float64(balance) +
			0.11*float64(layer) +
			0.10*float64(image) +
			0.09*float64(net) +
			0.04*float64(latency) +
			0.05*float64(taint) +
			0.04*float64(qos) +
			0.05*float64(affinity) +
			0.21*float64(runtimeLoad) +
			0.05*float64(topology)

	finalGood := int64(0)
	if weightedFinal >= 50 {
		finalGood = 1
	}

	runtimeGood := int64(0)
	if runtimeLoad >= 15 {
		runtimeGood = 1
	}

	balanceGood := int64(0)
	if balance >= 70 {
		balanceGood = 1
	}

	cpuGood := int64(0)
	if cpu >= 50 {
		cpuGood = 1
	}

	memoryGood := int64(0)
	if mem >= 50 {
		memoryGood = 1
	}

	storageGood := int64(0)
	if storage >= 50 {
		storageGood = 1
	}

	netGood := int64(0)
	if net >= 3 {
		netGood = 1
	}

	layerGood := int64(0)
	if layer >= 50 {
		layerGood = 1
	}

	imageGood := int64(0)
	if image >= 50 {
		imageGood = 1
	}

	taintGood := int64(0)
	if taint >= 40 {
		taintGood = 1
	}

	affinityGood := int64(0)
	if affinity >= 50 {
		affinityGood = 1
	}

	topologyGood := int64(0)
	if topology >= 50 {
		topologyGood = 1
	}

	c3 := 0
	c1 := cpuGood + memoryGood + storageGood + balanceGood
	if runtimeGood == 1 {
		c1 = runtimeGood + storageGood + balanceGood + netGood
	}

	c2 := netGood + layerGood + imageGood
	c4 := taintGood + affinityGood
	c5 := topologyGood

	rawCriticality := c1*25 + c2*15 + c4*5 + c5

	const maxRawCriticality int64 = 100 + 45 + 10 + 1 // 131

	criticalityScore := rawCriticality * 60 / maxRawCriticality
	tieBreaker := (net + layer + image + latency) * 40 / (3 * framework.MaxNodeScore)

	score := criticalityScore + tieBreaker
	if score < 0 {
		score = 0
	}
	if score > framework.MaxNodeScore {
		score = framework.MaxNodeScore
	}
	// final :=
	// 	0.05*float64(storage) +
	// 	0.25*float64(layer) +
	// 	0.25*float64(net) +
	// 	0.08*float64(taint) +
	// 	0.07*float64(qos) +
	// 	0.10*float64(affinity) +
	// 	0.20*float64(runtimeLoad)
	// final :=
	// 	0.07*float64(cpu) +
	// 		0.07*float64(mem) +
	// 		0.04*float64(storage) +
	// 		0.11*float64(balance) +
	// 		0.24*float64(layer) +
	// 		0.24*float64(net) +
	// 		0.07*float64(taint) +
	// 		0.06*float64(qos) +
	// 		0.10*float64(affinity)

	// final :=
	// 	0.09*float64(cpu) +
	// 		0.09*float64(mem) +
	// 		0.04*float64(storage) +
	// 		0.13*float64(balance) +
	// 		0.28*float64(layer) +
	// 		0.28*float64(net) +
	// 		0.09*float64(taint) + 0.05*float64(qos)

	// final :=
	// 	0.10*float64(cpu) +
	// 		0.10*float64(mem) +
	// 		0.05*float64(storage) +
	// 		0.15*float64(balance) +
	// 		0.30*float64(layer) +
	// 		0.30*float64(net)

	// final := (wLayer * float64(layer)) + (wNet * float64(net))

	// score := int64(final)
	// if score < 0 {
	// 	score = 0
	// }
	// if score > framework.MaxNodeScore {
	// 	score = framework.MaxNodeScore
	// }

	cpuAlloc := nodeInfo.GetAllocatable().GetMilliCPU()
	cpuUsed := nodeInfo.GetRequested().GetMilliCPU()
	cpuPod := podCPURequest(pod)
	cpuAfter := cpuUsed + cpuPod

	memAlloc := nodeInfo.GetAllocatable().GetMemory()
	memUsed := nodeInfo.GetRequested().GetMemory()
	memPod := podMemoryRequest(pod)
	memAfter := memUsed + memPod

	storageAlloc := nodeInfo.GetAllocatable().GetEphemeralStorage()
	storageUsed := nodeInfo.GetRequested().GetEphemeralStorage()
	storagePod := podStorageRequest(pod)
	storageAfter := storageUsed + storagePod
	// if runtimeGood == 0 || balanceGood == 0 {
	// 	score = tieBreaker
	// }
	fmt.Printf("CRITICALITY_SCORE_DETAIL "+
		"pod=%s node=%s "+
		"c1=%d "+
		"c2=%d "+
		"c3=%d "+
		"c4=%d "+
		"c5=%d "+
		"runtime_good=%d balance_good=%d "+
		"cpu_good=%d memory_good=%d storage_good=%d net_good=%d "+
		"layer_good=%d image_good=%d "+
		"taint_good=%d affinity_good=%d final_good=%d "+
		"topology_good=%d "+
		"criticality_score=%d "+
		"cpu_alloc_m=%d cpu_used_m=%d cpu_pod_m=%d cpu_after_m=%d cpu_score=%d "+
		"mem_alloc_bytes=%d mem_used_bytes=%d mem_pod_bytes=%d mem_after_bytes=%d mem_score=%d "+
		"storage_alloc_bytes=%d storage_used_bytes=%d storage_pod_bytes=%d storage_after_bytes=%d storage_score=%d "+
		"balance_score=%d "+
		"layer_score=%d "+
		"net_score=%d "+
		"latency_score=%d "+
		"taint_score=%d "+
		"qos_score=%d "+
		"affinity_score=%d "+
		"topology_score=%d "+
		"cpu_load_score=%d "+
		"mem_load_score=%d "+
		"runtime_load_score=%d "+
		"predicted_cpu=%.2f "+
		"predicted_mem=%.2f "+
		"final_score=%d\n",

		pod.Name,
		nodeInfo.Node().Name,
		c1,
		c2,
		c3,
		c4,
		c5,
		runtimeGood,
		balanceGood,
		cpuGood,
		memoryGood,
		storageGood,
		netGood,
		layerGood,
		imageGood,
		taintGood,
		affinityGood,
		finalGood,
		topologyGood,
		score,
		cpuAlloc,
		cpuUsed,
		cpuPod,
		cpuAfter,
		cpu,

		memAlloc,
		memUsed,
		memPod,
		memAfter,
		mem,

		storageAlloc,
		storageUsed,
		storagePod,
		storageAfter,
		storage,

		balance,
		layer,
		net,
		latency,
		taint,
		qos,
		affinity,
		topology,
		cpuLoad,
		memLoad,
		runtimeLoad,
		predictedCPU,
		predictedMem,
		score,
	)
	// fmt.Println(
	// 	"HYBRID_SCORE",
	// 	"pod:", pod.Name,	"net:", net,
	// 	"final:", score,
	// )
	// 	"node:", nodeInfo.Node().Name,
	// 	"layer:", layer,
	//

	return score, nil
}

func (h *HybridScore) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

func (h *HybridScore) refreshLoop(ctx context.Context) {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.refreshNodeLayers(ctx)
			h.refreshTraffic(ctx)
			h.refreshRuntimeMetrics(ctx)
			h.refreshImageCache(ctx)
			h.refreshLatency(ctx)
		}
	}
}

func (h *HybridScore) refreshLatency(ctx context.Context) {
	cm, err := h.handle.ClientSet().CoreV1().
		ConfigMaps(latencyNamespace).
		Get(ctx, latencyConfigMap, metav1.GetOptions{})
	if err != nil {
		fmt.Println("HYBRID_SCORE failed to read latency ConfigMap:", err)
		return
	}

	raw := cm.Data["latency.json"]
	if raw == "" {
		fmt.Println("HYBRID_SCORE latency.json missing")
		return
	}

	var data LatencyData
	if err := json.Unmarshal([]byte(raw), &data); err != nil {
		fmt.Println("HYBRID_SCORE failed to parse latency.json:", err)
		return
	}

	h.mu.Lock()
	h.latency = data
	h.mu.Unlock()
}

func (h *HybridScore) latencyScoreForNode(pod *v1.Pod, nodeInfo fwk.NodeInfo) int64 {
	node := nodeInfo.Node()
	if node == nil {
		return framework.MaxNodeScore
	}

	pendingWorkload := workloadName(pod)
	if pendingWorkload == "" {
		return framework.MaxNodeScore
	}

	h.mu.RLock()
	targets := h.traffic[pendingWorkload]
	h.mu.RUnlock()

	if len(targets) == 0 {
		return framework.MaxNodeScore
	}

	var weightedLatency float64
	var totalTraffic float64

	for _, existingPodInfo := range nodeInfo.GetPods() {
		existingPod := existingPodInfo.GetPod()
		if existingPod == nil || existingPod.Spec.NodeName == "" {
			continue
		}

		existingWorkload := workloadName(existingPod)
		trafficWeight, ok := targets[existingWorkload]
		if !ok {
			continue
		}

		var lat float64
		if node.Name == existingPod.Spec.NodeName {
			lat = 0
		} else {
			key := node.Name + "->" + existingPod.Spec.NodeName

			h.mu.RLock()
			v, ok := h.latency[key]
			h.mu.RUnlock()
			if !ok {
				v = 999
			}
			lat = v
		}

		weightedLatency += trafficWeight * lat
		totalTraffic += trafficWeight
	}

	if totalTraffic <= 0 {
		return framework.MaxNodeScore
	}

	avgLatency := weightedLatency / totalTraffic
	score := int64(100 - avgLatency)
	if score < 0 {
		return 0
	}
	if score > framework.MaxNodeScore {
		return framework.MaxNodeScore
	}
	return score
}

func (h *HybridScore) refreshNodeLayers(ctx context.Context) {
	cm, err := h.handle.ClientSet().CoreV1().
		ConfigMaps(nodeCacheNamespace).
		Get(ctx, nodeCacheConfigMap, metav1.GetOptions{})
	if err != nil {
		fmt.Println("HYBRID_SCORE failed to read layer cache ConfigMap:", err)
		return
	}

	newMap := map[string][]string{}

	for nodeName, raw := range cm.Data {
		var layers []string
		if err := json.Unmarshal([]byte(raw), &layers); err != nil {
			fmt.Println("HYBRID_SCORE failed to parse layers for node:", nodeName, "error:", err)
			continue
		}
		newMap[nodeName] = layers
	}

	h.mu.Lock()
	h.nodeLayers = newMap
	h.mu.Unlock()
}

func (h *HybridScore) refreshTraffic(ctx context.Context) {
	cm, err := h.handle.ClientSet().CoreV1().
		ConfigMaps(netmarksNamespace).
		Get(ctx, netmarksConfigMap, metav1.GetOptions{})
	if err != nil {
		fmt.Println("HYBRID_SCORE failed to read NetMARKS ConfigMap:", err)
		return
	}

	raw := cm.Data["traffic.json"]
	if raw == "" {
		fmt.Println("HYBRID_SCORE traffic.json missing in NetMARKS ConfigMap")
		return
	}

	var traffic TrafficData
	if err := json.Unmarshal([]byte(raw), &traffic); err != nil {
		fmt.Println("HYBRID_SCORE failed to parse traffic.json:", err)
		return
	}

	h.mu.Lock()
	h.traffic = traffic
	h.mu.Unlock()
}
func (h *HybridScore) imageScore(
	pod *v1.Pod,
	nodeInfo fwk.NodeInfo,
) int64 {

	nodeInfos, err :=
		h.handle.SnapshotSharedLister().
			NodeInfos().
			List()

	if err != nil {
		return 0
	}

	totalNodes := len(nodeInfos)

	var sum int64
	imageCount := 0

	images := nodeInfo.GetImageStates()

	for _, c := range pod.Spec.InitContainers {

		imageCount++

		state, ok := images[normalizeImageName(c.Image)]
		if !ok {
			continue
		}

		spread :=
			float64(state.NumNodes) /
				float64(totalNodes)

		sum += int64(
			float64(state.Size) * spread,
		)
	}

	for _, c := range pod.Spec.Containers {

		imageCount++

		state, ok := images[normalizeImageName(c.Image)]
		if !ok {
			continue
		}

		spread :=
			float64(state.NumNodes) /
				float64(totalNodes)

		sum += int64(
			float64(state.Size) * spread,
		)
	}

	if imageCount == 0 {
		return 0
	}

	const mb int64 = 1024 * 1024
	const minThreshold int64 = 23 * mb
	const maxContainerThreshold int64 = 1000 * mb

	maxThreshold :=
		maxContainerThreshold *
			int64(imageCount)

	if sum < minThreshold {
		sum = minThreshold
	} else if sum > maxThreshold {
		sum = maxThreshold
	}

	return framework.MaxNodeScore *
		(sum - minThreshold) /
		(maxThreshold - minThreshold)
}
func (h *HybridScore) layerScore(pod *v1.Pod, nodeInfo fwk.NodeInfo) int64 {
	nodeName := nodeInfo.Node().Name

	h.mu.RLock()
	nodeLayers := h.nodeLayers[nodeName]
	h.mu.RUnlock()

	var totalScore int64
	var imageCount int64

	containers := []v1.Container{}
	containers = append(containers, pod.Spec.InitContainers...)
	containers = append(containers, pod.Spec.Containers...)

	for _, c := range containers {
		imageName := normalizeImageName(c.Image)

		img, ok := h.images[imageName]
		fmt.Printf(
			"LAYER_DEBUG pod=%s node=%s image=%s image_found=%t node_layers=%d img_total_size=%d\n",
			pod.Name,
			nodeName,
			imageName,
			ok,
			len(nodeLayers),
			img.TotalSize,
		)
		if !ok || img.TotalSize <= 0 {
			continue
		}

		s := layerScoreForNode(img, nodeLayers)

		// Fallback: if registry-layer digest matching fails,
		// but Kubernetes says this node has the image, treat it as cached.
		if s == 0 {
			if s == 0 {
				fmt.Printf("LAYER_DIGEST_MATCH_ZERO pod=%s node=%s image=%s\n",
					pod.Name, nodeName, imageName)
			}
		}

		totalScore += s
		imageCount++
	}

	if imageCount == 0 {
		return 0
	}

	score := totalScore / imageCount
	if score < 0 {
		return 0
	}
	if score > framework.MaxNodeScore {
		return framework.MaxNodeScore
	}
	return score
}

func layerScoreForNode(img ImageMeta, nodeLayers []string) int64 {
	if img.TotalSize <= 0 {
		return 0
	}

	nodeLayerSet := map[string]bool{}
	for _, l := range nodeLayers {
		nodeLayerSet[l] = true
	}

	var matchedSize int64
	for _, l := range img.Layers {
		if nodeLayerSet[l.Layer] {
			matchedSize += l.Size
		}
	}

	score := matchedSize * framework.MaxNodeScore / img.TotalSize
	if score < 0 {
		return 0
	}
	if score > framework.MaxNodeScore {
		return framework.MaxNodeScore
	}
	fmt.Printf(
		"LAYER_MATCH_DEBUG img_total_size=%d matched_size=%d layer_score=%d node_layers=%d image_layers=%d\n",
		img.TotalSize,
		matchedSize,
		score,
		len(nodeLayers),
		len(img.Layers),
	)
	return score
}

func (h *HybridScore) netmarksScore(pod *v1.Pod, nodeInfo fwk.NodeInfo) int64 {
	pendingWorkload := workloadName(pod)
	if pendingWorkload == "" {
		return 0
	}

	h.mu.RLock()
	targets := h.traffic[pendingWorkload]
	h.mu.RUnlock()

	if len(targets) == 0 {
		return 0
	}

	var rawScore float64

	for _, existingPodInfo := range nodeInfo.GetPods() {
		existingPod := existingPodInfo.GetPod()
		if existingPod == nil {
			continue
		}

		existingWorkload := workloadName(existingPod)
		if existingWorkload == "" {
			continue
		}

		if weight, ok := targets[existingWorkload]; ok {
			rawScore += weight
		}
	}

	// Your earlier NetMARKS scorer used rawScore / 100000.
	// Keep that scaling for now so behavior stays close to your working plugin.
	score := int64(rawScore / 1000)

	if score < 0 {
		return 0
	}
	if score > framework.MaxNodeScore {
		return framework.MaxNodeScore
	}
	return score
}

func workloadName(pod *v1.Pod) string {
	if pod == nil {
		return ""
	}

	app := pod.Labels["app"]
	version := pod.Labels["version"]

	if app == "" {
		return ""
	}

	if version == "" {
		return app
	}

	base := app + "-" + version

	// Your collector currently emits keys like productpage-v1-v1.
	// So make scheduler use the same key format.
	if strings.HasSuffix(app, "-"+version) {
		return app + "-" + version
	}

	return base + "-" + version
}

// func loadImageCache() (map[string]ImageMeta, error) {
// 	data, err := os.ReadFile(imageCachePath)
// 	if err != nil {
// 		return nil, err
// 	}

// 	var cache Cache
// 	if err := json.Unmarshal(data, &cache); err != nil {
// 		return nil, err
// 	}

// 	if cache.Images == nil {
// 		cache.Images = map[string]ImageMeta{}
// 	}

// 	return cache.Images, nil
// }

func normalizeImageName(name string) string {
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == ':' {
			return name
		}
		if name[i] == '/' {
			break
		}
	}
	return name + ":latest"
}
func (h *HybridScore) refreshRuntimeMetrics(ctx context.Context) {
	cm, err := h.handle.ClientSet().CoreV1().
		ConfigMaps(runtimeNamespace).
		Get(ctx, runtimeConfigMap, metav1.GetOptions{})
	if err != nil {
		fmt.Println("HYBRID_SCORE failed to read runtime metrics ConfigMap:", err)
		return
	}

	raw := cm.Data["metrics.json"]
	if raw == "" {
		fmt.Println("HYBRID_SCORE metrics.json missing in runtime metrics ConfigMap")
		return
	}

	var metrics RuntimeMetrics
	if err := json.Unmarshal([]byte(raw), &metrics); err != nil {
		fmt.Println("HYBRID_SCORE failed to parse metrics.json:", err)
		return
	}

	h.mu.Lock()
	h.runtimeMetrics = metrics
	h.mu.Unlock()
}
func (h *HybridScore) runtimeLoadScore(
	pod *v1.Pod,
	nodeInfo fwk.NodeInfo,
	ps *hybridPreScoreState,
) (int64, int64, int64, float64, float64) {
	node := nodeInfo.Node()
	if node == nil {
		return 0, 0, 0, 0, 0
	}

	h.mu.RLock()
	metric, ok := h.runtimeMetrics[node.Name]
	h.mu.RUnlock()

	if !ok {
		return 0, 0, 0, 0, 0
	}

	cpuPodPercent := podCPURequestPercent(
		ps.podCPU,
		nodeInfo.GetAllocatable().GetMilliCPU(),
	)

	memPodPercent := podMemoryRequestPercent(
		ps.podMemory,
		nodeInfo.GetAllocatable().GetMemory(),
	)

	predictedCPU := metric.CPUPercent + cpuPodPercent
	predictedMem := metric.MemoryPercent + memPodPercent

	cpuLoadScore := trimaranTargetScore(predictedCPU, targetCPUUtilization)
	memLoadScore := trimaranTargetScore(predictedMem, targetMemoryUtilization)

	// Conservative: node is only as good as its worst loaded resource.
	runtimeScore := cpuLoadScore
	if memLoadScore < runtimeScore {
		runtimeScore = memLoadScore
	}

	return cpuLoadScore, memLoadScore, runtimeScore, predictedCPU, predictedMem
}
func (h *HybridScore) refreshImageCache(ctx context.Context) {
	cm, err := h.handle.ClientSet().CoreV1().
		ConfigMaps(imageCacheNamespace).
		Get(ctx, imageCacheConfigMap, metav1.GetOptions{})
	if err != nil {
		fmt.Println("HYBRID_SCORE failed to read image cache ConfigMap:", err)
		return
	}

	raw := cm.Data["cache.json"]
	if raw == "" {
		fmt.Println("HYBRID_SCORE cache.json missing in image cache ConfigMap")
		return
	}

	var cache Cache
	if err := json.Unmarshal([]byte(raw), &cache); err != nil {
		fmt.Println("HYBRID_SCORE failed to parse image cache:", err)
		return
	}

	if cache.Images == nil {
		cache.Images = map[string]ImageMeta{}
	}

	h.mu.Lock()
	h.images = cache.Images
	h.mu.Unlock()
}
