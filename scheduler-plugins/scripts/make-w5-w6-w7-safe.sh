#!/bin/bash
set -e

SCHEDS="default criticality clustercriticality clusterhybrid hybridmerged"
NS="sched-eval"

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

clean_ns() {
  kubectl create ns "$NS" --dry-run=client -o yaml | kubectl apply -f -
  kubectl delete pod -n "$NS" --all --force --grace-period=0 --ignore-not-found=true >/dev/null 2>&1 || true
  sleep 5
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

  mkdir -p "$BASE/$S"
  clean_ns

  local SCHED_NAME
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

  kubectl wait -n "$NS" --for=condition=PodScheduled pod -l "workload=$PREFIX" --timeout=90s || true

  kubectl get pods -n "$NS" -o wide -l "workload=$PREFIX" > "$BASE/$S/placement.txt"

  local DEPLOY
  DEPLOY=$(scheduler_deploy "$S")

  if [ "$DEPLOY" != "" ]; then
    kubectl logs -n kube-system deploy/"$DEPLOY" --since=10m > "$BASE/$S/scheduler.log" 2>/dev/null || true
  else
    echo "default scheduler: no custom score log" > "$BASE/$S/scheduler.log"
  fi

  clean_ns
}

analyze_runtime() {
  local BASE="$1"
  local OUT="$BASE/summary"
  mkdir -p "$OUT"

  echo -e "scheduler\toverloaded_node_pods\tlow_load_node_pods\ttotal_scheduled_pods" > "$OUT/runtime-avoidance.tsv"

  for S in $SCHEDS; do
    P="$BASE/$S/placement.txt"

    OVER=$(awk 'NR>1 && ($7=="playground-cluster-worker" || $7=="playground-cluster-worker2") {c++} END{print c+0}' "$P" 2>/dev/null)
    LOW=$(awk 'NR>1 && ($7=="playground-cluster-worker3" || $7=="playground-cluster-worker4" || $7=="playground-cluster-worker5") {c++} END{print c+0}' "$P" 2>/dev/null)
    TOTAL=$(awk 'NR>1 && $7!="<none>" && $7!="" {c++} END{print c+0}' "$P" 2>/dev/null)

    echo -e "$S\t$OVER\t$LOW\t$TOTAL" >> "$OUT/runtime-avoidance.tsv"
  done

  column -t -s $'\t' "$OUT/runtime-avoidance.tsv" > "$OUT/runtime-avoidance-readable.txt"
}

run_w5() {
  local BASE="results/W5-runtime"
  rm -rf "$BASE"
  mkdir -p "$BASE"

  make_runtime_cache

  for S in $SCHEDS; do
    echo "Running W5 runtime adaptation for $S"
    run_workload "$BASE" "$S" 8 "w5-$S"
  done

  analyze_runtime "$BASE"
  cat "$BASE/summary/runtime-avoidance-readable.txt"
}

run_w6() {
  local BASE="results/W6-cluster-scale"
  rm -rf "$BASE"
  mkdir -p "$BASE"

  make_runtime_cache

  for N in 2 3 5; do
    echo "Running W6 cluster-scale node-count=$N"

    for node in $(kubectl get nodes --no-headers | awk '/worker/ {print $1}'); do
      kubectl label node "$node" w6-active- --overwrite >/dev/null 2>&1 || true
    done

    kubectl get nodes --no-headers | awk '/worker/ {print $1}' | head -"$N" | while read node; do
      kubectl label node "$node" w6-active=true --overwrite >/dev/null
    done

    for S in $SCHEDS; do
      run_workload "$BASE/nodes-$N" "$S" 6 "w6-n${N}-$S"
    done

    analyze_runtime "$BASE/nodes-$N"
  done

  echo "W6 done."
}

run_w7() {
  local BASE="results/W7-workload-scale"
  rm -rf "$BASE"
  mkdir -p "$BASE"

  make_runtime_cache

  for C in 8 16 32; do
    echo "Running W7 workload-scale pod-count=$C"

    for S in $SCHEDS; do
      run_workload "$BASE/pods-$C" "$S" "$C" "w7-p${C}-$S"
    done

    analyze_runtime "$BASE/pods-$C"
  done

  echo "W7 done."
}

case "$1" in
  w5) run_w5 ;;
  w6) run_w6 ;;
  w7) run_w7 ;;
  all) run_w5; run_w6; run_w7 ;;
  *) echo "Usage: $0 {w5|w6|w7|all}" ;;
esac
