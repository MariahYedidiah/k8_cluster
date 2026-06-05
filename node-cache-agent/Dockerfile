FROM golang:1.26 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY main.go .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o node-cache-agent main.go

FROM alpine:latest
COPY --from=builder /app/node-cache-agent /node-cache-agent
ENTRYPOINT ["/node-cache-agent"]
