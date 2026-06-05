import json
import os
import time
import urllib.request
import subprocess

REGISTRY = os.environ.get("REGISTRY", "http://headache:5000")
NAMESPACE = os.environ.get("NAMESPACE", "kube-system")
CONFIGMAP = os.environ.get("CONFIGMAP", "lrs-image-cache")
INTERVAL = int(os.environ.get("INTERVAL", "20"))

def get_json(url, headers=None):
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read().decode())

def build_cache():
    catalog = get_json(f"{REGISTRY}/v2/_catalog")
    images = {}

    for repo in catalog.get("repositories", []):
        tags_data = get_json(f"{REGISTRY}/v2/{repo}/tags/list")
        tags = tags_data.get("tags") or []

        for tag in tags:
            image_name = f"localhost:5000/{repo}:{tag}"

            headers = {
                "Accept": "application/vnd.docker.distribution.manifest.v2+json"
            }

            try:
                manifest = get_json(
                    f"{REGISTRY}/v2/{repo}/manifests/{tag}",
                    headers=headers
                )
            except Exception as e:
                print("REGISTRY_WATCHER manifest error", repo, tag, repr(e), flush=True)
                continue

            total = 0
            layers = []

            for layer in manifest.get("layers", []):
                digest = layer.get("digest")
                size = int(layer.get("size", 0))
                if digest:
                    layers.append({"layer": digest, "size": size})
                    total += size

            images[image_name] = {
                "total_size": total,
                "layers": layers
            }

    return {"images": images}

def apply_configmap(cache):
    payload = json.dumps(cache, indent=2)

    cm = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": CONFIGMAP,
            "namespace": NAMESPACE
        },
        "data": {
            "cache.json": payload
        }
    }

    p = subprocess.run(
        ["kubectl", "apply", "-f", "-"],
        input=json.dumps(cm),
        text=True,
        capture_output=True
    )

    if p.returncode != 0:
        raise RuntimeError(p.stderr)

    print("REGISTRY_WATCHER updated", CONFIGMAP, "images=", len(cache["images"]), flush=True)
    print(payload, flush=True)

while True:
    try:
        cache = build_cache()
        apply_configmap(cache)
    except Exception as e:
        print("REGISTRY_WATCHER error:", repr(e), flush=True)

    time.sleep(INTERVAL)
