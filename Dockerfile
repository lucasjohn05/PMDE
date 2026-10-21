# Build: a static binary, so the runtime image needs no libc.
FROM golang:1.27.1 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/pmde ./cmd/pmde
# An empty data directory owned by the runtime user: a named volume
# mounted at /data starts out with this ownership, so pmde can write it.
RUN mkdir -p /out/data

# Run: distroless static has CA certificates (for the wss:// venues),
# no shell and no package manager, and runs as uid/gid 65532 ("nonroot").
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/pmde /usr/local/bin/pmde
COPY --from=build --chown=65532:65532 /out/data /data
COPY pairs.yaml /etc/pmde/pairs.yaml
USER 65532:65532
VOLUME /data
# Metrics. Publish it on the host's loopback only (see deploy/).
EXPOSE 9100
# SIGTERM (docker stop) is handled: pmde closes the open windows as
# partial, flushes, fsyncs and closes its files before exiting.
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/pmde", "-config", "/etc/pmde/pairs.yaml", "-data-dir", "/data", "-metrics-addr", ":9100"]
