import json
import os
import time
import urllib.parse
import urllib.request
import subprocess

PROM_URL = os.environ.get("PROM_URL", "http://prometheus.istio-system.svc.cluster.local:9090")
NAMESPACE = os.environ.get("NAMESPACE", "kube-system")
CONFIGMAP = os.environ.get("CONFIGMAP", "runtime-node-metrics-cache")
INTERVAL = int(os.environ.get("INTERVAL", "15"))

CPU_QUERY = '''
100 *
sum by (instance) (
  rate(node_cpu_seconds_total{mode!="idle"}[1m])
)
/
sum by (instance) (
  rate(node_cpu_seconds_total[1m])
)
'''.strip()

MEM_QUERY = '''
100 *
(1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes))
'''.strip()

def run(cmd):
    return subprocess.check_output(cmd, text=True)

def prom_query(query):
    url = PROM_URL + "/api/v1/query?" + urllib.parse.urlencode({"query": query})
    with urllib.request.urlopen(url, timeout=10) as r:
        data = json.loads(r.read().decode())
    if data.get("status") != "success":
        raise RuntimeError(data)
    return data["data"]["result"]

def parse_cpu_to_m(cpu):
    cpu = cpu.strip()
    if cpu.endswith("m"):
        return float(cpu[:-1])
    return float(cpu) * 1000.0

def parse_mem_to_mi(mem):
    mem = mem.strip()
    low = mem.lower()
    if low.endswith("ki"):
        return float(mem[:-2]) / 1024.0
    if low.endswith("mi"):
        return float(mem[:-2])
    if low.endswith("gi"):
        return float(mem[:-2]) * 1024.0
    return float(mem) / 1024.0 / 1024.0

def node_maps():
    raw = run(["kubectl", "get", "nodes", "-o", "json"])
    data = json.loads(raw)

    ip_to_name = {}
    alloc = {}

    for item in data["items"]:
        name = item["metadata"]["name"]

        for addr in item["status"].get("addresses", []):
            if addr["type"] in ["InternalIP", "Hostname"]:
                ip_to_name[addr["address"]] = name

        cpu = item["status"]["allocatable"]["cpu"]
        mem = item["status"]["allocatable"]["memory"]
        alloc[name] = {
            "cpu_m": parse_cpu_to_m(cpu),
            "mem_mi": parse_mem_to_mi(mem),
        }

    return ip_to_name, alloc

def clean_instance(instance):
    return instance.split(":")[0]

def from_prometheus():
    ip_to_name, _ = node_maps()

    cpu = {}
    mem = {}

    for item in prom_query(CPU_QUERY):
        inst = clean_instance(item["metric"].get("instance", ""))
        node = ip_to_name.get(inst, inst)
        cpu[node] = float(item["value"][1])

    for item in prom_query(MEM_QUERY):
        inst = clean_instance(item["metric"].get("instance", ""))
        node = ip_to_name.get(inst, inst)
        mem[node] = float(item["value"][1])

    out = {}
    for node in sorted(set(cpu.keys()) | set(mem.keys())):
        out[node] = {
            "cpu_percent": round(cpu.get(node, 0.0), 2),
            "memory_percent": round(mem.get(node, 0.0), 2),
            "source": "prometheus"
        }

    if not out:
        raise RuntimeError("Prometheus returned no node runtime metrics")

    return out

def from_metrics_api():
    _, alloc = node_maps()
    raw = run(["kubectl", "top", "nodes", "--no-headers"])

    out = {}

    for line in raw.splitlines():
        p = line.split()
        if len(p) < 5:
            continue

        node = p[0]
        cpu_m = parse_cpu_to_m(p[1])
        mem_mi = parse_mem_to_mi(p[3])

        if node not in alloc:
            continue

        out[node] = {
            "cpu_percent": round(cpu_m * 100.0 / alloc[node]["cpu_m"], 2),
            "memory_percent": round(mem_mi * 100.0 / alloc[node]["mem_mi"], 2),
            "source": "metrics-api"
        }

    if not out:
        raise RuntimeError("Metrics API returned no node runtime metrics")

    return out

def update_configmap(metrics):
    payload = json.dumps(metrics, indent=2)

    cm = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": CONFIGMAP,
            "namespace": NAMESPACE,
        },
        "data": {
            "metrics.json": payload,
        },
    }

    subprocess.run(
        ["kubectl", "apply", "-f", "-"],
        input=json.dumps(cm),
        text=True,
        check=True,
    )

    print("HYBRID_RUNTIME_COLLECTOR updated", CONFIGMAP, flush=True)
    print(payload, flush=True)

while True:
    try:
        try:
            metrics = from_prometheus()
        except Exception as e:
            print("Prometheus runtime unavailable, falling back to Metrics API:", repr(e), flush=True)
            metrics = from_metrics_api()

        update_configmap(metrics)

    except Exception as e:
        print("HYBRID_RUNTIME_COLLECTOR error:", repr(e), flush=True)

    time.sleep(INTERVAL)
