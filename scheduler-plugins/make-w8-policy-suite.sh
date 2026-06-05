#!/bin/bash
SCHED="$1"
OUT="$2"
mkdir -p "$OUT"

cat > "$OUT/workload.yaml" <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: w8-node-affinity
  namespace: policy-eval
spec:
  schedulerName: ${SCHED}
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: affinity-target
            operator: In
            values: ["yes"]
  containers:
  - name: pause
    image: registry.k8s.io/pause:3.10
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: w8-pod-affinity
  namespace: policy-eval
spec:
  replicas: 4
  selector:
    matchLabels:
      app: w8-pod-affinity
  template:
    metadata:
      labels:
        app: w8-pod-affinity
    spec:
      schedulerName: ${SCHED}
      affinity:
        podAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
          - weight: 100
            podAffinityTerm:
              labelSelector:
                matchLabels:
                  app: w8-pod-affinity
              topologyKey: kubernetes.io/hostname
      containers:
      - name: pause
        image: registry.k8s.io/pause:3.10
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: w8-pod-antiaffinity
  namespace: policy-eval
spec:
  replicas: 6
  selector:
    matchLabels:
      app: w8-pod-antiaffinity
  template:
    metadata:
      labels:
        app: w8-pod-antiaffinity
    spec:
      schedulerName: ${SCHED}
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
          - weight: 100
            podAffinityTerm:
              labelSelector:
                matchLabels:
                  app: w8-pod-antiaffinity
              topologyKey: kubernetes.io/hostname
      containers:
      - name: pause
        image: registry.k8s.io/pause:3.10
---
apiVersion: v1
kind: Pod
metadata:
  name: w8-taint-intolerant
  namespace: policy-eval
spec:
  schedulerName: ${SCHED}
  containers:
  - name: pause
    image: registry.k8s.io/pause:3.10
---
apiVersion: v1
kind: Pod
metadata:
  name: w8-taint-tolerant
  namespace: policy-eval
spec:
  schedulerName: ${SCHED}
  tolerations:
  - key: "special"
    operator: "Equal"
    value: "avoid"
    effect: "PreferNoSchedule"
  containers:
  - name: pause
    image: registry.k8s.io/pause:3.10
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: w8-topology
  namespace: policy-eval
spec:
  replicas: 6
  selector:
    matchLabels:
      app: w8-topology
  template:
    metadata:
      labels:
        app: w8-topology
    spec:
      schedulerName: ${SCHED}
      topologySpreadConstraints:
      - maxSkew: 1
        topologyKey: topology.kubernetes.io/zone
        whenUnsatisfiable: ScheduleAnyway
        labelSelector:
          matchLabels:
            app: w8-topology
      containers:
      - name: pause
        image: registry.k8s.io/pause:3.10
YAML
