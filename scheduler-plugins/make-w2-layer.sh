#!/bin/bash

SCHED="$1"
OUT="$2"
IMG="$3"

mkdir -p "$OUT"

cat > "$OUT/workload.yaml" <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: layer-cached
  namespace: layer-eval
spec:
  replicas: 6
  selector:
    matchLabels:
      app: layer-cached
  template:
    metadata:
      labels:
        app: layer-cached
    spec:
      schedulerName: ${SCHED}
      containers:
      - name: cached-image
        image: ${IMG}
        imagePullPolicy: IfNotPresent
        command: ["sleep", "3600"]
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: layer-nginx
  namespace: layer-eval
spec:
  replicas: 6
  selector:
    matchLabels:
      app: layer-nginx
  template:
    metadata:
      labels:
        app: layer-nginx
    spec:
      schedulerName: ${SCHED}
      containers:
      - name: nginx
        image: nginx:1.25
        imagePullPolicy: IfNotPresent
        command: ["sleep", "3600"]
YAML
