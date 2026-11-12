# syntax=docker/dockerfile:1
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
# ponytail: no VERSION build-arg; cmd/durableq exposes no version var yet.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -buildid=" -o /durableq ./cmd/durableq

FROM gcr.io/distroless/static-debian12:nonroot
LABEL org.opencontainers.image.title="durableq" \
      org.opencontainers.image.description="Postgres-backed durable job queue (API + worker)" \
      org.opencontainers.image.source="https://github.com/Eliot-2006/durableq" \
      org.opencontainers.image.licenses="MIT"
COPY --from=build /durableq /durableq
USER 65532:65532
# Defaults to the API port. Worker containers override the URL to :9090
# (compose: healthcheck.test in docker-compose.yml; k8s uses probes instead).
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD ["/durableq", "healthcheck", "--url", "http://127.0.0.1:8080/healthz"]
ENTRYPOINT ["/durableq"]
