FROM python:3.11-slim

RUN apt-get update && apt-get install -y curl ca-certificates && rm -rf /var/lib/apt/lists/*

RUN curl -LO "https://dl.k8s.io/release/v1.35.1/bin/linux/amd64/kubectl" \
 && chmod +x kubectl \
 && mv kubectl /usr/local/bin/kubectl

COPY registry_watcher.py /app/registry_watcher.py

ENTRYPOINT ["python3", "/app/registry_watcher.py"]
