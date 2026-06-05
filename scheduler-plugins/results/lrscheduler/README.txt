LRScheduler / HybridMerged Real Kubernetes Validation

Validated components:
1. Registry watcher populated real image metadata into lrs-image-cache.
2. Node cache agent populated real node layer metadata into lrs-node-layer-cache.
3. HybridMerged produced non-zero layer_score using real layer caches.
4. NetMARKS traffic cache was read by HybridMerged.
5. productpage-netmarks-test produced non-zero net_score.
6. HybridMerged combined resource, layer, network, taint, QoS, affinity, runtime, and topology scores.

Key evidence:
- lrs-image-cache.json
- lrs-node-layer-cache.json
- registry-watcher.log
- hybrid-score-detail-test-nginx.txt
- hybrid-score-detail-productpage-netmarks.txt
- netmarks-validation-summary.txt
- real-cache-test-summary.txt
