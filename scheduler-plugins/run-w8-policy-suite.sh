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

kubectl label node playground-cluster-worker topology.kubernetes.io/zone=zone-a --overwrite
kubectl label node playground-cluster-worker2 topology.kubernetes.io/zone=zone-a --overwrite
kubectl label node playground-cluster-worker3 topology.kubernetes.io/zone=zone-a --overwrite
kubectl label node playground-cluster-worker4 topology.kubernetes.io/zone=zone-b --overwrite
kubectl label node playground-cluster-worker5 topology.kubernetes.io/zone=zone-b --overwrite
kubectl label node playground-cluster-worker6 topology.kubernetes.io/zone=zone-b --overwrite

for SHORT in default criticality clustercriticality clusterhybrid hybridmerged; do
  SCHED="${SCHEDS[$SHORT]}"
  OUT="results/W8-policy-suite/$SHORT"
  mkdir -p "$OUT"

  echo "===== Running W8 with $SCHED ====="

  kubectl delete ns policy-eval --ignore-not-found
  sleep 5
  kubectl create ns policy-eval
  kubectl label ns policy-eval istio-injection=disabled --overwrite || true

  ./make-w8-policy-suite.sh "$SCHED" "$OUT"
  kubectl apply -f "$OUT/workload.yaml"

  sleep 20

  kubectl get pods -n policy-eval -o wide > "$OUT/placement.txt"
  kubectl describe pods -n policy-eval > "$OUT/events.txt"

  if [ "$SHORT" = "default" ]; then
    POD=$(kubectl -n kube-system get pods | awk '/kube-scheduler/ {print $1; exit}')
    kubectl -n kube-system logs "$POD" --since=20m > "$OUT/scheduler.log" || true
  else
    kubectl -n kube-system logs deploy/$SCHED --since=20m > "$OUT/scheduler.log" || true
    kubectl -n kube-system get deploy $SCHED -o yaml > "$OUT/deployment-live.yaml" || true
  fi
done
