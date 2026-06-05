package hybridscore

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const Name = "HybridScore"

const imageCachePath = "/etc/lrs/cache.json"
const nodeCacheNamespace = "kube-system"
const nodeCacheConfigMap = "lrs-node-layer-cache"

type Layer struct {
	Layer string `json:"layer"`
	Size  int64  `json:"size"`
}

type ImageMeta struct {
	TotalSize int64   `json:"total_size"`
	Layers    []Layer `json:"layers"`
}

type Cache struct {
	Images map[string]ImageMeta `json:"images"`
}

type HybridScore struct {
	handle framework.Handle

	mu         sync.RWMutex
	images     map[string]ImageMeta
	nodeLayers map[string][]string
}

var _ = framework.ScorePlugin(&HybridScore{})

func New(ctx context.Context, _ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	images, err := loadImageCache()
	if err != nil {
		return nil, err
	}

	p := &HybridScore{
		handle:     h,
		images:     images,
		nodeLayers: map[string][]string{},
	}

	p.refreshNodeLayers(ctx)
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
	nodeName := nodeInfo.Node().Name

	h.mu.RLock()
	nodeLayers := h.nodeLayers[nodeName]
	h.mu.RUnlock()

	var totalScore int64
	var imageCount int64

	for _, c := range pod.Spec.Containers {
		imageName := normalizeImageName(c.Image)

		img, ok := h.images[imageName]
		if !ok || img.TotalSize == 0 {
			continue
		}

		score := layerScoreForNode(img, nodeLayers)
		totalScore += score
		imageCount++
	}

	if imageCount == 0 {
		return 0, nil
	}

	return totalScore / imageCount, nil
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
		}
	}
}

func (h *HybridScore) refreshNodeLayers(ctx context.Context) {
	cm, err := h.handle.ClientSet().CoreV1().
		ConfigMaps(nodeCacheNamespace).
		Get(ctx, nodeCacheConfigMap, metav1.GetOptions{})
	if err != nil {
		return
	}

	newMap := map[string][]string{}

	for nodeName, raw := range cm.Data {
		var layers []string
		if err := json.Unmarshal([]byte(raw), &layers); err != nil {
			continue
		}
		newMap[nodeName] = layers
	}

	h.mu.Lock()
	h.nodeLayers = newMap
	h.mu.Unlock()
}

func layerScoreForNode(img ImageMeta, nodeLayers []string) int64 {
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

	return matchedSize * 100 / img.TotalSize
}

func loadImageCache() (map[string]ImageMeta, error) {
	data, err := os.ReadFile(imageCachePath)
	if err != nil {
		return nil, err
	}

	var cache Cache
	if err := json.Unmarshal(data, &cache); err != nil {
		return nil, err
	}

	return cache.Images, nil
}

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

func (h *HybridScore) ScoreExtensions() framework.ScoreExtensions {
	return nil
}
