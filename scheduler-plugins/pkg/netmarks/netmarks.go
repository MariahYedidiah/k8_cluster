package netmarks

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const Name = "NetMARKS"

const netmarksNamespace = "kube-system"
const netmarksConfigMap = "netmarks-traffic-cache"

type TrafficData map[string]map[string]float64

type NetMARKS struct {
	handle  framework.Handle
	traffic TrafficData
}

var _ framework.ScorePlugin = &NetMARKS{}

func New(ctx context.Context, _ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	p := &NetMARKS{
		handle:  h,
		traffic: TrafficData{},
	}
	p.refreshTraffic(ctx)
	return p, nil
}

func (n *NetMARKS) Name() string {
	return Name
}

func (n *NetMARKS) Score(
	ctx context.Context,
	state fwk.CycleState,
	pod *v1.Pod,
	nodeInfo fwk.NodeInfo,
) (int64, *fwk.Status) {
	n.refreshTraffic(ctx)

	nodeName := nodeInfo.Node().Name
	pendingWorkload := workloadName(pod)

	fmt.Println("NETMARKS scoring pod:", pod.Name, "workload:", pendingWorkload, "node:", nodeName)

	podsOnNode, err := n.handle.ClientSet().CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + nodeName,
	})
	if err != nil {
		fmt.Println("NETMARKS error listing pods:", err)
		return 0, nil
	}

	var rawScore float64

	if targets, ok := n.traffic[pendingWorkload]; ok {
		for _, existingPod := range podsOnNode.Items {
			existingWorkload := workloadName(&existingPod)
			if existingWorkload == "" {
				continue
			}

			if weight, ok := targets[existingWorkload]; ok {
				rawScore += weight
				fmt.Println("NETMARKS matched:", pendingWorkload, "->", existingWorkload, "on node", nodeName, "weight:", weight)
			}
		}
	}

	score := int64(rawScore / 100000)

	if score > 100 {
		score = 100
	}

	fmt.Println("NETMARKS final score pod:", pod.Name, "workload:", pendingWorkload, "node:", nodeName, "score:", score)

	return score, nil
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
func (n *NetMARKS) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

func (n *NetMARKS) refreshTraffic(ctx context.Context) {
	cm, err := n.handle.ClientSet().CoreV1().
		ConfigMaps(netmarksNamespace).
		Get(ctx, netmarksConfigMap, metav1.GetOptions{})
	if err != nil {
		return
	}

	raw := cm.Data["traffic.json"]

	var traffic TrafficData
	if err := json.Unmarshal([]byte(raw), &traffic); err != nil {
		return
	}

	n.traffic = traffic
}
