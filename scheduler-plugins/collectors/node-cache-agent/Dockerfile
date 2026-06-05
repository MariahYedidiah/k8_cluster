FROM python:3.11-slim

RUN apt-get update && apt-get install -y curl

RUN curl -LO https://dl.k8s.io/release/v1.35.1/bin/linux/amd64/kubectl \
 && chmod +x kubectl \
 && mv kubectl /usr/local/bin/

COPY node_cache_agent.py /app/

ENTRYPOINT ["python3","/app/node_cache_agent.py"]
