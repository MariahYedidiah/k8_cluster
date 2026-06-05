package hybridscore

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const Name = "HybridScore"

const imageCacheNamespace = "kube-system"
const imageCacheConfigMap = "lrs-image-cache"

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

var _ framework.ScorePlugin = &HybridScore{}

func New(ctx context.Context, _ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	p := &HybridScore{
		handle:     h,
		images:     map[string]ImageMeta{},
		nodeLayers: map[string][]string{},
	}

	p.refreshImageCache(ctx)
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
	node := nodeInfo.Node()
	if node == nil {
		return 0, nil
	}

	nodeName := node.Name

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

		h.mu.RLock()
		img, ok := h.images[imageName]
		h.mu.RUnlock()

		if !ok || img.TotalSize <= 0 {
			continue
		}

		s := layerScoreForNode(img, nodeLayers)

		if s == 0 {
			if imageStates := nodeInfo.GetImageStates(); imageStates != nil {
				if _, ok := imageStates[imageName]; ok {
					s = framework.MaxNodeScore
				}
			}
		}

		totalScore += s
		imageCount++
	}

	if imageCount == 0 {
		fmt.Printf(
			"HYBRID_SCORE_DETAIL pod=%s node=%s image_count=0 layer_score=0 final_score=0\n",
			pod.Name,
			nodeName,
		)
		return 0, nil
	}

	score := totalScore / imageCount

	if score < 0 {
		score = 0
	}
	if score > framework.MaxNodeScore {
		score = framework.MaxNodeScore
	}

	fmt.Printf(
		"HYBRID_SCORE_DETAIL pod=%s node=%s image_count=%d layer_score=%d final_score=%d\n",
		pod.Name,
		nodeName,
		imageCount,
		score,
		score,
	)

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
			h.refreshImageCache(ctx)
			h.refreshNodeLayers(ctx)
		}
	}
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

func (h *HybridScore) refreshNodeLayers(ctx context.Context) {
	cm, err := h.handle.ClientSet().CoreV1().
		ConfigMaps(nodeCacheNamespace).
		Get(ctx, nodeCacheConfigMap, metav1.GetOptions{})
	if err != nil {
		fmt.Println("HYBRID_SCORE failed to read node layer cache ConfigMap:", err)
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

	return score
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