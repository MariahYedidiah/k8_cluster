package clusterhybrid

import (
	"math"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	fwk "k8s.io/kube-scheduler/framework"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

func podAntiAffinityScore(pod *v1.Pod, nodeInfo fwk.NodeInfo) int64 {
	if pod == nil {
		return 0
	}

	affinity := pod.Spec.Affinity
	if affinity == nil ||
		affinity.PodAntiAffinity == nil ||
		len(affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution) == 0 {
		return framework.MaxNodeScore
	}

	var totalWeight int64
	var conflictWeight int64

	for _, term := range affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
		weight := int64(term.Weight)
		totalWeight += weight

		for _, existing := range nodeInfo.GetPods() {
			existingPod := existing.GetPod()
			if existingPod == nil {
				continue
			}

			if labelsMatchPodAffinityTerm(existingPod, term.PodAffinityTerm) {
				conflictWeight += weight
				break
			}
		}
	}

	if totalWeight == 0 {
		return framework.MaxNodeScore
	}

	score := (totalWeight - conflictWeight) * framework.MaxNodeScore / totalWeight
	if score < 0 {
		return 0
	}
	if score > framework.MaxNodeScore {
		return framework.MaxNodeScore
	}
	return score
}

func labelsMatchPodAffinityTerm(existingPod *v1.Pod, term v1.PodAffinityTerm) bool {
	if existingPod == nil || term.LabelSelector == nil {
		return false
	}

	selector, err := metav1.LabelSelectorAsSelector(term.LabelSelector)
	if err != nil {
		return false
	}

	return selector.Matches(labels.Set(existingPod.Labels))
}

func podCPURequest(pod *v1.Pod) int64 {
	var appTotal int64
	for _, c := range pod.Spec.Containers {
		appTotal += c.Resources.Requests.Cpu().MilliValue()
	}

	var maxInit int64
	for _, c := range pod.Spec.InitContainers {
		v := c.Resources.Requests.Cpu().MilliValue()
		if v > maxInit {
			maxInit = v
		}
	}

	if maxInit > appTotal {
		return maxInit
	}
	return appTotal
}

func podMemoryRequest(pod *v1.Pod) int64 {
	var appTotal int64
	for _, c := range pod.Spec.Containers {
		appTotal += c.Resources.Requests.Memory().Value()
	}

	var maxInit int64
	for _, c := range pod.Spec.InitContainers {
		v := c.Resources.Requests.Memory().Value()
		if v > maxInit {
			maxInit = v
		}
	}

	if maxInit > appTotal {
		return maxInit
	}
	return appTotal
}

func podStorageRequest(pod *v1.Pod) int64 {
	var appTotal int64
	for _, c := range pod.Spec.Containers {
		appTotal += c.Resources.Requests.StorageEphemeral().Value()
	}

	var maxInit int64
	for _, c := range pod.Spec.InitContainers {
		v := c.Resources.Requests.StorageEphemeral().Value()
		if v > maxInit {
			maxInit = v
		}
	}

	if maxInit > appTotal {
		return maxInit
	}
	return appTotal
}

func leastAllocatedScore(alloc, used, podReq int64) int64 {
	if alloc <= 0 {
		return 0
	}

	requested := used + podReq
	if requested >= alloc {
		return 0
	}

	return (alloc - requested) * framework.MaxNodeScore / alloc
}

func balanceScore(nodeInfo fwk.NodeInfo, pod *v1.Pod) int64 {
	cpuAlloc := nodeInfo.GetAllocatable().GetMilliCPU()
	memAlloc := nodeInfo.GetAllocatable().GetMemory()

	if cpuAlloc <= 0 || memAlloc <= 0 {
		return 0
	}

	cpuReq := nodeInfo.GetRequested().GetMilliCPU() + podCPURequest(pod)
	memReq := nodeInfo.GetRequested().GetMemory() + podMemoryRequest(pod)

	cpuFraction := float64(cpuReq) / float64(cpuAlloc)
	memFraction := float64(memReq) / float64(memAlloc)

	if cpuFraction > 1 {
		cpuFraction = 1
	}
	if memFraction > 1 {
		memFraction = 1
	}

	std := math.Abs(cpuFraction-memFraction) / 2
	score := int64((1 - std) * float64(framework.MaxNodeScore))

	if score < 0 {
		return 0
	}
	if score > framework.MaxNodeScore {
		return framework.MaxNodeScore
	}

	return score
}
func qosScore(pod *v1.Pod) int64 {
	switch v1qos.GetPodQOS(pod) {

	case v1.PodQOSGuaranteed:
		return framework.MaxNodeScore

	case v1.PodQOSBurstable:
		return 50

	case v1.PodQOSBestEffort:
		return 10

	default:
		return 0
	}
}
func nodeAffinityScore(pod *v1.Pod, node *v1.Node) int64 {
	if pod == nil || node == nil {
		return 0
	}

	affinity := pod.Spec.Affinity
	if affinity == nil ||
		affinity.NodeAffinity == nil ||
		len(affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution) == 0 {
		return framework.MaxNodeScore
	}

	var totalWeight int64
	var matchedWeight int64

	for _, term := range affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
		weight := int64(term.Weight)
		totalWeight += weight

		if nodeSelectorTermMatches(term.Preference, node.Labels) {
			matchedWeight += weight
		}
	}

	if totalWeight == 0 {
		return framework.MaxNodeScore
	}

	return matchedWeight * framework.MaxNodeScore / totalWeight
}

func nodeSelectorTermMatches(term v1.NodeSelectorTerm, labels map[string]string) bool {
	for _, expr := range term.MatchExpressions {
		nodeValue, exists := labels[expr.Key]

		switch expr.Operator {
		case v1.NodeSelectorOpIn:
			if !exists || !containsString(expr.Values, nodeValue) {
				return false
			}
		case v1.NodeSelectorOpNotIn:
			if exists && containsString(expr.Values, nodeValue) {
				return false
			}
		case v1.NodeSelectorOpExists:
			if !exists {
				return false
			}
		case v1.NodeSelectorOpDoesNotExist:
			if exists {
				return false
			}
		default:
			return false
		}
	}

	return true
}

func containsString(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}
func trimaranTargetScore(predictedUtil, targetUtil float64) int64 {
	if predictedUtil < 0 {
		predictedUtil = 0
	}
	if predictedUtil > 100 {
		predictedUtil = 100
	}

	var score float64

	if predictedUtil <= targetUtil {
		// Below target: packing is good.
		score = (predictedUtil / targetUtil) * float64(framework.MaxNodeScore)
	} else {
		// Above target: penalize overload aggressively.
		over := predictedUtil - targetUtil
		remaining := 100.0 - targetUtil
		if remaining <= 0 {
			return 0
		}
		score = (1.0 - over/remaining) * float64(framework.MaxNodeScore)
	}

	if score < 0 {
		return 0
	}
	if score > float64(framework.MaxNodeScore) {
		return framework.MaxNodeScore
	}
	return int64(score)
}

func podCPURequestPercent(podCPUm int64, nodeCPUm int64) float64 {
	if nodeCPUm <= 0 {
		return 0
	}
	return float64(podCPUm) * 100.0 / float64(nodeCPUm)
}

func podMemoryRequestPercent(podMemBytes int64, nodeMemBytes int64) float64 {
	if nodeMemBytes <= 0 {
		return 0
	}
	return float64(podMemBytes) * 100.0 / float64(nodeMemBytes)
}
