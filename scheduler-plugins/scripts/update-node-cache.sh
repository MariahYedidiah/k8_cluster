#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${1:-playground-cluster}"
CACHE_FILE="${2:-lrs-cache/cache.json}"
TMP_FILE="$(mktemp)"

if [ ! -f "$CACHE_FILE" ]; then
  echo "ERROR: cache file not found: $CACHE_FILE"
  exit 1
fi

python3 - "$CLUSTER_NAME" "$CACHE_FILE" "$TMP_FILE" <<'PY'
import json
import re
import subprocess
import sys

cluster_name = sys.argv[1]
cache_file = sys.argv[2]
tmp_file = sys.argv[3]

def run(cmd):
    return subprocess.check_output(cmd, text=True)

def extract_layers_from_node(node):
    output = run(["docker", "exec", node, "ctr", "-n", "k8s.io", "content", "ls"])
    layers = set()

    # containerd labels look like:
    # containerd.io/gc.ref.content.l.0=sha256:abc...
    for digest in re.findall(r"containerd\.io/gc\.ref\.content\.l\.\d+=(sha256:[a-f0-9]+)", output):
        layers.add(digest)

    return sorted(layers)

nodes = run(["kind", "get", "nodes", "--name", cluster_name]).splitlines()

with open(cache_file, "r") as f:
    cache = json.load(f)

cache["nodes"] = {}

for node in nodes:
    cache["nodes"][node] = extract_layers_from_node(node)

with open(tmp_file, "w") as f:
    json.dump(cache, f, indent=2)

PY

mv "$TMP_FILE" "$CACHE_FILE"
echo "Updated node layer cache in $CACHE_FILE"
