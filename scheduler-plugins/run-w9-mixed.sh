#!/bin/bash
set -e

declare -A SCHEDS
SCHEDS[default]="default-scheduler"
SCHEDS[criticality]="criticality-scheduler"
SCHEDS[clustercriticality]="clustercriticality-scheduler"
SCHEDS[clusterhybrid]="clusterhybrid-scheduler"
SCHEDS[hybridmerged]="hybridmerged-scheduler"

kubectl label node playground-cluster-worker2 affinity-target=yes --overwrite
kubectl taint node playground-cluster-worker6 special=avoid:PreferNoSchedule --overwrite || true

for SHORT in default criticality clustercriticality clusterhybrid hybridmerged; do
  SCHED="${SCHEDS[$SHORT]}"
  OUT="results/W9-mixed-proportional/$SHORT"
  mkdir -p "$OUT"

  echo "===== Running W9 with $SCHED ====="

  kubectl delete ns mixed-eval --ignore-not-found
  sleep 5
  kubectl create ns mixed-eval
  kubectl label ns mixed-eval istio-injection=disabled --overwrite || true

  IMG=$(kubectl get cm lrs-image-cache -n kube-system -o jsonpath='{.data.cache\.json}' | jq -r '.images | keys[0]')

  ./make-w9-mixed.sh "$SCHED" "$OUT" "$IMG"
  kubectl apply -f "$OUT/workload.yaml"

  sleep 30

  kubectl get pods -n mixed-eval -o wide > "$OUT/placement.txt"
  kubectl describe pods -n mixed-eval > "$OUT/events.txt"

  if [ "$SHORT" = "default" ]; then
    POD=$(kubectl -n kube-system get pods | awk '/kube-scheduler/ {print $1; exit}')
    kubectl -n kube-system logs "$POD" --since=20m > "$OUT/scheduler.log" || true
  else
    kubectl -n kube-system logs deploy/$SCHED --since=20m > "$OUT/scheduler.log" || true
    kubectl -n kube-system get deploy $SCHED -o yaml > "$OUT/deployment-live.yaml" || true
  fi

  kubectl get cm lrs-image-cache -n kube-system -o yaml > "$OUT/lrs-image-cache.yaml" 2>/dev/null || true
  kubectl get cm lrs-node-layer-cache -n kube-system -o yaml > "$OUT/lrs-node-layer-cache.yaml" 2>/dev/null || true
  kubectl get cm netmarks-traffic-cache -n kube-system -o yaml > "$OUT/netmarks-cache.yaml" 2>/dev/null || true
  kubectl get cm runtime-node-metrics-cache -n kube-system -o yaml > "$OUT/runtime-cache.yaml" 2>/dev/null || true
done
