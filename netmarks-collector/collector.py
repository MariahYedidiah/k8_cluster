import requests
import json
import subprocess
import time

PROM_URL = "http://localhost:9090/api/v1/query"

QUERY = 'sum(istio_requests_total) by (source_workload,destination_workload)'

while True:
    try:
        response = requests.get(
            PROM_URL,
            params={"query": QUERY},
            timeout=10,
        )

        data = response.json()

        traffic = {}

        for item in data["data"]["result"]:
            metric = item["metric"]

            src = metric.get("source_workload", "")
            dst = metric.get("destination_workload", "")

            if not src or not dst:
                continue

            value = float(item["value"][1])

            if src not in traffic:
                traffic[src] = {}

            traffic[src][dst] = value

        traffic_json = json.dumps(traffic, indent=2)

        with open("/tmp/traffic.json", "w") as f:
            f.write(traffic_json)

        subprocess.run([
            "kubectl",
            "create",
            "configmap",
            "netmarks-traffic-cache",
            "-n",
            "kube-system",
            "--from-file=traffic.json=/tmp/traffic.json",
            "--dry-run=client",
            "-o",
            "yaml",
        ], stdout=open("/tmp/cm.yaml", "w"))

        subprocess.run([
            "kubectl",
            "apply",
            "-f",
            "/tmp/cm.yaml",
        ])

        print("updated traffic cache")

    except Exception as e:
        print("collector error:", e)

    time.sleep(30)
