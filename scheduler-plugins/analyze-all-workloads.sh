#!/bin/bash
set -e

SCHEDS="default criticality clustercriticality clusterhybrid hybridmerged"

mkdir -p results/thesis-summary

echo "W1 Policy Correctness" > results/thesis-summary/W1-policy-summary.txt
for S in $SCHEDS; do
  echo "===== $S =====" >> results/thesis-summary/W1-policy-summary.txt
  grep -E "affinity_score|taint_score|topology_score|qos_score|cpu_score|mem_score|storage_score" \
    results/W1-condition/$S/scheduler.log | head -30 >> results/thesis-summary/W1-policy-summary.txt || true
  awk 'NR>1 && $3=="Running" {print $1,$7}' results/W1-condition/$S/placement.txt \
    >> results/thesis-summary/W1-policy-summary.txt
done

echo "W2 Layer Locality" > results/thesis-summary/W2-layer-summary.txt
for S in $SCHEDS; do
  echo "===== $S =====" >> results/thesis-summary/W2-layer-summary.txt
  grep "layer_score" results/W2-layer/$S/scheduler.log | grep -v "layer_score=0" | head -30 \
    >> results/thesis-summary/W2-layer-summary.txt || true
  awk 'NR>1 && $1 ~ /layer/ && $3=="Running" {print $7}' results/W2-layer/$S/placement.txt | sort | uniq -c \
    >> results/thesis-summary/W2-layer-summary.txt || true
done

echo "W3 Network Locality" > results/thesis-summary/W3-network-summary.txt
cat results/W3-network/summary/w3-netmarks-thesis-summary.txt >> results/thesis-summary/W3-network-summary.txt
grep -R "net_score" results/W3-network/*/scheduler.log | grep -v "net_score=0" | head -40 \
  >> results/thesis-summary/W3-network-summary.txt || true

echo "W4 Combined Optimization" > results/thesis-summary/W4-hybrid-summary.txt
for S in $SCHEDS; do
  echo "===== $S =====" >> results/thesis-summary/W4-hybrid-summary.txt
  grep -E "layer_score|net_score|cpu_score|mem_score|balance_score|topology_score|runtime_load_score|final_score" \
    results/W4-hybrid/$S/scheduler.log | head -50 >> results/thesis-summary/W4-hybrid-summary.txt || true
  echo "--- node counts ---" >> results/thesis-summary/W4-hybrid-summary.txt
  awk 'NR>1 && $3=="Running" {print $7}' results/W4-hybrid/$S/placement.txt | sort | uniq -c \
    >> results/thesis-summary/W4-hybrid-summary.txt || true
done

echo "Done. Files saved in results/thesis-summary/"
