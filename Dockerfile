# QuorumLog image: static Go binaries on a distroless base.
# Plain Dockerfile features only (no BuildKit-specific syntax), so it builds
# the same with `docker build`, docker compose, Render and CI.

# ---- build ----
FROM golang:1.26 AS build
WORKDIR /src
# Download modules in their own layer so code changes don't refetch them.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/qlogd ./cmd/qlogd \
 && CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/qlog ./cmd/qlog \
 && mkdir -p /out/data

# ---- run: no shell, no package manager, uid 65532 ----
# static-debian13 is the maintained distroless line; static-debian12 tags are
# no longer updated (github.com/GoogleContainerTools/distroless).
FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/qlogd /out/qlog /usr/local/bin/
# /data must belong to the nonroot user; a new named volume mounted here
# copies this ownership.
COPY --from=build --chown=65532:65532 /out/data /data
USER 65532:65532
ENV PORT=8080
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/qlogd"]
# Demo mode: 3 Raft nodes in one process, dashboard + API on $PORT.
# deploy/docker-compose.yml overrides these arguments to run one real node
# per container.
CMD ["--demo", "--data-dir", "/data"]
