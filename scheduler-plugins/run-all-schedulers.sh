#!/bin/bash
set -e

WORKLOAD="$1"

if [ -z "$WORKLOAD" ]; then
  echo "Usage: ./run-all-schedulers.sh W1-condition|W2-layer|W3-network|W4-hybrid|W5-runtime"
  exit 1
fi

declare -A SCHEDS
SCHEDS[default]="default-scheduler"
SCHEDS[criticality]="criticality-scheduler"
SCHEDS[clustercriticality]="clustercriticality-scheduler"
SCHEDS[clusterhybrid]="clusterhybrid-scheduler"
SCHEDS[hybridmerged]="hybridmerged-scheduler"

for SHORT in default criticality clustercriticality clusterhybrid hybridmerged; do
  SCHED="${SCHEDS[$SHORT]}"
  OUT="results/$WORKLOAD/$SHORT"
  mkdir -p "$OUT"

  echo "===== Running $WORKLOAD with $SCHED ====="

  kubectl delete pod -n sched-tests --all --ignore-not-found
  kubectl delete deploy -n sched-tests --all --ignore-not-found
  kubectl delete pod -n layer-eval --all --ignore-not-found 2>/dev/null || true
  kubectl delete deploy -n layer-eval --all --ignore-not-found 2>/dev/null || true
  kubectl delete pod -n hybrid-eval --all --ignore-not-found 2>/dev/null || true
  kubectl delete deploy -n hybrid-eval --all --ignore-not-found 2>/dev/null || true

  case "$WORKLOAD" in
    W1-condition)
      ./make-w1-condition.sh "$SCHED" "$OUT"
      kubectl apply -f "$OUT/workload.yaml"
      NS="sched-tests"
      ;;

    W2-layer)
      IMG=$(kubectl get cm lrs-image-cache -n kube-system -o jsonpath='{.data.cache\.json}' | jq -r '.images | keys[0]')
      ./make-w2-layer.sh "$SCHED" "$OUT" "$IMG"
      kubectl apply -f "$OUT/workload.yaml"
      NS="layer-eval"
      ;;

    W3-network)
      ./make-w3-network.sh "$SCHED" "$OUT"
      kubectl apply -f "$OUT/workload.yaml"
      NS="bookinfo-net"
      ;;

    W4-hybrid)
      IMG=$(kubectl get cm lrs-image-cache -n kube-system -o jsonpath='{.data.cache\.json}' | jq -r '.images | keys[0]')
      ./make-w4-hybrid.sh "$SCHED" "$OUT" "$IMG"
      kubectl apply -f "$OUT/workload.yaml"
      NS="hybrid-eval"
      ;;

    *)
      echo "Unknown workload: $WORKLOAD"
      exit 1
      ;;
  esac

  sleep 15

  kubectl get pods -n "$NS" -o wide > "$OUT/placement.txt"
  kubectl describe pods -n "$NS" > "$OUT/events.txt"

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
