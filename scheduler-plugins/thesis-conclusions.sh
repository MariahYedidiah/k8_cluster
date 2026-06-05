#!/bin/bash

echo "===== THESIS RESULT CONCLUSIONS ====="

echo
echo "W1 Policy Correctness:"
echo "Conclusion: W1 checks whether schedulers respect CPU, memory, storage, QoS, affinity, taints, and topology."
echo "Use W1 mainly as correctness proof, not performance superiority."

echo
echo "W2 Layer Locality:"
for S in default criticality clustercriticality clusterhybrid hybridmerged; do
  C=$(grep -h "layer_score" results/W2-layer/$S/scheduler.log 2>/dev/null | grep -vc "layer_score=0" || true)
  echo "$S: nonzero layer-score lines = $C"
done
echo "Conclusion: scheduler(s) with many nonzero layer_score lines are using layer-cache information."

echo
echo "W3 Network Locality:"
cat results/W3-network/summary/w3-netmarks-thesis-summary.txt
echo "Conclusion: higher selected-highest-net-score count means better NetMARKS-aware placement."

echo
echo "W4 Combined Optimization:"
for S in default criticality clustercriticality clusterhybrid hybridmerged; do
  L=$(grep -h "layer_score" results/W4-hybrid/$S/scheduler.log 2>/dev/null | grep -vc "layer_score=0" || true)
  N=$(grep -h "net_score" results/W4-hybrid/$S/scheduler.log 2>/dev/null | grep -vc "net_score=0" || true)
  R=$(grep -h "runtime_load_score" results/W4-hybrid/$S/scheduler.log 2>/dev/null | wc -l || true)
  echo "$S: layer-used=$L network-used=$N runtime-used=$R"
done
echo "Conclusion: W4 shows which scheduler combines layer + network + runtime signals."

echo
echo "W5/W6/W7:"
echo "Not concluded unless you run runtime-stress / scale-stress workloads."
