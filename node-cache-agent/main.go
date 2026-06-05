package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/containerd/containerd"

	"github.com/containerd/containerd/images"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	namespace     = "kube-system"
	configMapName = "lrs-node-layer-cache"
	socketPath    = "/run/containerd/containerd.sock"
)

func main() {
	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		panic("NODE_NAME env var missing")
	}

	for {
		err := runOnce(nodeName)
		if err != nil {
			fmt.Println("ERROR:", err)
		}
		time.Sleep(20 * time.Second)
	}
}

func runOnce(nodeName string) error {
	layers, err := readContainerdLayers()
	if err != nil {
		return err
	}
	if err := writeConfigMap(nodeName, layers); err != nil {
		return err
	}
	fmt.Printf("updated %s with %d layers\n", nodeName, len(layers))
	return nil
}

func readContainerdLayers() ([]string, error) {
	ctx := context.Background()

	client, err := containerd.New(socketPath, containerd.WithDefaultNamespace("k8s.io"))
	if err != nil {
		return nil, err
	}
	defer client.Close()

	layerSet := map[string]bool{}

	imgs, err := client.ListImages(ctx)
	if err != nil {
		return nil, err
	}

	store := client.ContentStore()

	for _, img := range imgs {
		target := img.Target()

		manifest, err := images.Manifest(ctx, store, target, nil)
		if err != nil {
			fmt.Printf("NODE_CACHE manifest read failed image=%s target=%s err=%v\n",
				img.Name(), target.Digest.String(), err)
			continue
		}

		for _, layer := range manifest.Layers {
			d := layer.Digest.String()
			if d == "" {
				continue
			}

			layerSet[d] = true
			fmt.Printf("NODE_LAYER image=%s digest=%s size=%d\n",
				img.Name(), d, layer.Size)
		}
	}

	layers := make([]string, 0, len(layerSet))
	for l := range layerSet {
		layers = append(layers, l)
	}

	sort.Strings(layers)
	return layers, nil
}

// func readContainerdLayers() ([]string, error) {
// 	ctx := context.Background()

// 	client, err := containerd.New(socketPath, containerd.WithDefaultNamespace("k8s.io"))
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer client.Close()

// 	layerSet := map[string]bool{}

// 	err = client.ContentStore().Walk(ctx, func(info content.Info) error {
// 		for k, v := range info.Labels {
// 			if strings.HasPrefix(k, "containerd.io/gc.ref.content.l.") {
// 				layerSet[v] = true
// 				fmt.Printf(
// 					"NODE_LAYER_DEBUG key=%s digest=%s\n",
// 					k,
// 					v,
// 				)
// 			}
// 		}
// 		return nil
// 	})
// 	if err != nil {
// 		return nil, err
// 	}

// 	layers := []string{}
// 	for l := range layerSet {
// 		layers = append(layers, l)
// 	}
// 	return layers, nil
// }

func writeConfigMap(nodeName string, layers []string) error {
	ctx := context.Background()

	cfg, err := rest.InClusterConfig()
	if err != nil {
		return err
	}

	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}

	dataBytes, err := json.Marshal(layers)
	if err != nil {
		return err
	}

	cmClient := clientset.CoreV1().ConfigMaps(namespace)

	cm, err := cmClient.Get(ctx, configMapName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      configMapName,
				Namespace: namespace,
			},
			Data: map[string]string{
				nodeName: string(dataBytes),
			},
		}
		_, err = cmClient.Create(ctx, cm, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}

	if cm.Data == nil {
		cm.Data = map[string]string{}
	}

	cm.Data[nodeName] = string(dataBytes)

	_, err = cmClient.Update(ctx, cm, metav1.UpdateOptions{})
	return err
}
