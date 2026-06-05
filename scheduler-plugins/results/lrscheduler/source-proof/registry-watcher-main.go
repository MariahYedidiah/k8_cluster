package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	registryURL   = "http://headache:5000"
	namespace     = "kube-system"
	configMapName = "lrs-image-cache"
	configMapKey  = "cache.json"
)

type Catalog struct {
	Repositories []string `json:"repositories"`
}

type Tags struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

type Manifest struct {
	SchemaVersion int `json:"schemaVersion"`
	Layers []struct {
		MediaType string `json:"mediaType"`
		Size      int64  `json:"size"`
		Digest    string `json:"digest"`
	} `json:"layers"`
}

type LayerInfo struct {
	Layer string `json:"layer"`
	Size  int64  `json:"size"`
}

type ImageInfo struct {
	TotalSize int64       `json:"total_size"`
	Layers    []LayerInfo `json:"layers"`
}

type Cache struct {
	Images map[string]ImageInfo `json:"images"`
}

func getJSON(url string, accept string, out interface{}) error {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}

	if accept != "" {
		req.Header.Set("Accept", accept)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("GET %s failed: %s body=%s", url, resp.Status, string(body))
	}

	return json.Unmarshal(body, out)
}

func getCatalog() ([]string, error) {
	var c Catalog
	err := getJSON(registryURL+"/v2/_catalog", "", &c)
	return c.Repositories, err
}

func getTags(repo string) ([]string, error) {
	var t Tags
	err := getJSON(registryURL+"/v2/"+repo+"/tags/list", "", &t)
	return t.Tags, err
}

func getManifest(repo, tag string) (*Manifest, error) {
	accept := strings.Join([]string{
		"application/vnd.oci.image.manifest.v1+json",
		"application/vnd.docker.distribution.manifest.v2+json",
	}, ",")

	var m Manifest
	err := getJSON(registryURL+"/v2/"+repo+"/manifests/"+tag, accept, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

func buildImageCache() (*Cache, error) {
	cache := &Cache{
		Images: map[string]ImageInfo{},
	}

	repos, err := getCatalog()
	if err != nil {
		return nil, err
	}

	for _, repo := range repos {
		tags, err := getTags(repo)
		if err != nil {
			fmt.Println("[WARN] tags failed:", repo, err)
			continue
		}

		for _, tag := range tags {
			m, err := getManifest(repo, tag)
			if err != nil {
				fmt.Println("[WARN] manifest failed:", repo, tag, err)
				continue
			}

			info := ImageInfo{}

			for _, layer := range m.Layers {
				info.TotalSize += layer.Size
				info.Layers = append(info.Layers, LayerInfo{
					Layer: layer.Digest,
					Size:  layer.Size,
				})
			}

			imageKey1 := repo + ":" + tag
			imageKey2 := "localhost:5000/" + repo + ":" + tag
			imageKey3 := "headache:5000/" + repo + ":" + tag

			cache.Images[imageKey1] = info
			cache.Images[imageKey2] = info
			cache.Images[imageKey3] = info

			fmt.Println("[image]", imageKey1, "layers=", len(info.Layers), "size=", info.TotalSize)
		}
	}

	return cache, nil
}

func upsertConfigMap(client kubernetes.Interface, cache *Cache) error {
	ctx := context.Background()

	b, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return err
	}

	data := map[string]string{
		configMapKey: string(b),
	}

	cm, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, configMapName, metav1.GetOptions{})

	if err != nil {
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      configMapName,
				Namespace: namespace,
			},
			Data: data,
		}

		_, err = client.CoreV1().ConfigMaps(namespace).Create(ctx, cm, metav1.CreateOptions{})
		return err
	}

	cm.Data = data
	_, err = client.CoreV1().ConfigMaps(namespace).Update(ctx, cm, metav1.UpdateOptions{})
	return err
}

func main() {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		panic(err)
	}

	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		panic(err)
	}

	for {
		cache, err := buildImageCache()
		if err != nil {
			fmt.Println("[ERROR]", err)
		} else {
			err = upsertConfigMap(client, cache)
			if err != nil {
				fmt.Println("[ERROR] configmap:", err)
			} else {
				fmt.Println("[OK] updated", configMapName, "images=", len(cache.Images))
			}
		}

		time.Sleep(30 * time.Second)
	}
}
