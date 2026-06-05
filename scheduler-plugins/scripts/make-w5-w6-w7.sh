#!/bin/bash
set -e

SCHEDS="default criticality clustercriticality clusterhybrid hybridmerged"

scheduler_name() {
  case "$1" in
    default) echo "" ;;
    criticality) echo "criticality-scheduler" ;;
    clustercriticality) echo "clustercriticality-scheduler" ;;
    clusterhybrid) echo "clusterhybrid-scheduler" ;;
    hybridmerged) echo "hybrid-scheduler5" ;;
  esac
}

scheduler_deploy() {
  case "$1" in
    default) echo "" ;;
    criticality) echo "criticality-scheduler" ;;
    clustercriticality) echo "clustercriticality-scheduler" ;;
    clusterhybrid) echo "clusterhybrid-scheduler" ;;
    hybridmerged) echo "hybrid-scheduler5" ;;
  esac
}

make_runtime_cache() {
cat <<CM | kubectl apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: runtime-node-metrics-cache
  namespace: kube-system
data:
  metrics.json: |
    {
      "playground-cluster-worker":  { "cpu_percent": 90, "memory_percent": 85 },
      "playground-cluster-worker2": { "cpu_percent": 80, "memory_percent": 75 },
      "playground-cluster-worker3": { "cpu_percent": 20, "memory_percent": 30 },
      "playground-cluster-worker4": { "cpu_percent": 15, "memory_percent": 25 },
      "playground-cluster-worker5": { "cpu_percent": 10, "memory_percent": 20 }
    }
CM
}

run_workload() {
  local BASE="$1"
  local S="$2"
  local COUNT="$3"
  local PREFIX="$4"

  NS="sched-eval"
  mkdir -p "$BASE/$S"

  kubectl create ns "$NS" --dry-run=client -o yaml | kubectl apply -f -
  kubectl delete pod -n "$NS" -l "workload=$PREFIX" --ignore-not-found=true

  SCHED_NAME=$(scheduler_name "$S")

  for i in $(seq 1 "$COUNT"); do
    if [ "$SCHED_NAME" = "" ]; then
      SCHED_LINE=""
    else
      SCHED_LINE="schedulerName: $SCHED_NAME"
    fi

cat <<YAML | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: $PREFIX-$i
  namespace: $NS
  labels:
    app: $PREFIX
    workload: $PREFIX
spec:
  $SCHED_LINE
  restartPolicy: Never
  containers:
  - name: pause
    image: registry.k8s.io/pause:3.9
    resources:
      requests:
        cpu: "50m"
        memory: "64Mi"
YAML
  done

  kubectl wait -n "$NS" --for=condition=Ready pod -l "workload=$PREFIX" --timeout=180s || true
  kubectl get pods -n "$NS" -o wide -l "workload=$PREFIX" > "$BASE/$S/placement.txt"

  DEPLOY=$(scheduler_deploy "$S")
  if [ "$DEPLOY" != "" ]; then
    kubectl logs -n kube-system deploy/"$DEPLOY" --since=30m > "$BASE/$S/scheduler.log" 2>/dev/null || true
  else
    echo "default scheduler: no custom score log" > "$BASE/$S/scheduler.log"
  fi
}

analyze_node_avoidance() {
  local BASE="$1"
  local OUT="$BASE/summary"
  mkdir -p "$OUT"

  echo -e "scheduler\toverloaded_node_pods\tlow_load_node_pods\ttotal_pods" > "$OUT/runtime-avoidance.tsv"

  for S in $SCHEDS; do
    P="$BASE/$S/placement.txt"

    OVER=$(awk 'NR>1 && $3=="Running" && ($7=="playground-cluster-worker" || $7=="playground-cluster-worker2") {c++} END{print c+0}' "$P")
    LOW=$(awk 'NR>1 && $3=="Running" && ($7=="playground-cluster-worker3" || $7=="playground-cluster-worker4" || $7=="playground-cluster-worker5") {c++} END{print c+0}' "$P")
    TOTAL=$(awk 'NR>1 && $3=="Running" {c++} END{print c+0}' "$P")

    echo -e "$S\t$OVER\t$LOW\t$TOTAL" >> "$OUT/runtime-avoidance.tsv"
  done

  column -t -s $'\t' "$OUT/runtime-avoidance.tsv" > "$OUT/runtime-avoidance-readable.txt"
}

run_w5() {
  local BASE="results/W5-runtime"
  mkdir -p "$BASE"

  make_runtime_cache

  for S in $SCHEDS; do
    echo "Running W5 for $S"
    run_workload "$BASE" "$S" 20 "w5-runtime-$S"
  done

  analyze_node_avoidance "$BASE"

  echo "W5 saved:"
  echo "$BASE/summary/runtime-avoidance-readable.txt"
}

run_w6() {
  local BASE="results/W6-cluster-scale"
  mkdir -p "$BASE"

  COUNTS="2 3 4 5"

  for N in $COUNTS; do
    echo "===== W6 node-count=$N ====="

    for node in $(kubectl get nodes --no-headers | awk '/worker/ {print $1}'); do
      kubectl label node "$node" w6-active- --overwrite >/dev/null 2>&1 || true
    done

    kubectl get nodes --no-headers | awk '/worker/ {print $1}' | head -"$N" | while read node; do
      kubectl label node "$node" w6-active=true --overwrite
    done

    for S in $SCHEDS; do
      run_workload "$BASE/nodes-$N" "$S" 20 "w6-n${N}-$S"
    done

    analyze_node_avoidance "$BASE/nodes-$N"
  done

  echo "W6 saved under $BASE"
}

run_w7() {
  local BASE="results/W7-workload-scale"
  mkdir -p "$BASE"

  POD_COUNTS="12 24 48 96"

  for C in $POD_COUNTS; do
    echo "===== W7 pod-count=$C ====="

    for S in $SCHEDS; do
      run_workload "$BASE/pods-$C" "$S" "$C" "w7-p${C}-$S"
    done

    analyze_node_avoidance "$BASE/pods-$C"
  done

  echo "W7 saved under $BASE"
}

case "$1" in
  w5) run_w5 ;;
  w6) run_w6 ;;
  w7) run_w7 ;;
  all) run_w5; run_w6; run_w7 ;;
  *) echo "Usage: $0 {w5|w6|w7|all}" ;;
esac
