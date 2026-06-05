import json
import os
import subprocess
import time

NODE_NAME = os.environ["NODE_NAME"]
NAMESPACE = "kube-system"
CONFIGMAP = "lrs-node-layer-cache"

def collect_layers():
    p = subprocess.run(
        ["ctr","-n","k8s.io","content","ls"],
        capture_output=True,
        text=True
    )

    layers = []

    for line in p.stdout.splitlines():
        parts = line.split()
        if len(parts) > 0 and parts[0].startswith("sha256:"):
            layers.append(parts[0])

    return sorted(list(set(layers)))

def update_cm(layers):
    payload = json.dumps(layers)

    patch = {
        "data": {
            NODE_NAME: payload
        }
    }

    subprocess.run([
        "kubectl","patch","configmap",
        CONFIGMAP,
        "-n",NAMESPACE,
        "--type","merge",
        "-p",json.dumps(patch)
    ])

while True:
    try:
        update_cm(collect_layers())
        print("updated", NODE_NAME, flush=True)
    except Exception as e:
        print(e, flush=True)

    time.sleep(20)
