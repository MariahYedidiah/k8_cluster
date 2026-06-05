import json
import os
import time
import urllib.parse
import urllib.request
import subprocess

PROM_URL = os.environ.get("PROM_URL", "http://prometheus.istio-system.svc.cluster.local:9090")
NAMESPACE = os.environ.get("NAMESPACE", "kube-system")
CONFIGMAP = os.environ.get("CONFIGMAP", "netmarks-traffic-cache")
INTERVAL = int(os.environ.get("INTERVAL", "20"))

QUERY = '''
sum(istio_requests_total)
by (source_workload, destination_workload)
'''.strip()

def prom_query(query):
    url = PROM_URL + "/api/v1/query?" + urllib.parse.urlencode({"query": query})
    with urllib.request.urlopen(url, timeout=10) as r:
        data = json.loads(r.read().decode())
    if data.get("status") != "success":
        raise RuntimeError(data)
    return data["data"]["result"]

def normalize_workload(name):
    if not name or name == "unknown":
        return ""
    parts = name.split("-")
    if len(parts) >= 2 and parts[-1].startswith("v"):
        return name + "-" + parts[-1]
    return name

def build_traffic():
    result = prom_query(QUERY)
    traffic = {}

    for item in result:
        m = item.get("metric", {})
        value = float(item.get("value", [0, "0"])[1])

        src = normalize_workload(m.get("source_workload", ""))
        dst = normalize_workload(m.get("destination_workload", ""))

        if not src or not dst or src == dst:
            continue

        traffic.setdefault(src, {})
        traffic[src][dst] = traffic[src].get(dst, 0.0) + value

    return traffic

def apply_configmap(traffic):
    payload = json.dumps(traffic, indent=2)

    cm = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": CONFIGMAP,
            "namespace": NAMESPACE
        },
        "data": {
            "traffic.json": payload
        }
    }

    p = subprocess.run(
        ["kubectl", "apply", "-f", "-"],
        input=json.dumps(cm),
        text=True,
        capture_output=True
    )

    if p.returncode != 0:
        raise RuntimeError(p.stderr)

    print("NETMARKS_COLLECTOR updated", CONFIGMAP, flush=True)
    print(payload, flush=True)

while True:
    try:
        traffic = build_traffic()
        apply_configmap(traffic)
    except Exception as e:
        print("NETMARKS_COLLECTOR error:", repr(e), flush=True)

    time.sleep(INTERVAL)
