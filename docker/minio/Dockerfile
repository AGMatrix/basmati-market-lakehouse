# syntax=docker/dockerfile:1
#
# MinIO built from source.
# MinIO stopped publishing community images in late 2025 and archived its GitHub repos
# (minio/minio, minio/mc). The AGPL source is still public, so we compile the last
# release ourselves. This is a local dev object store only: an archived project gets no
# security fixes, so don't expose it beyond localhost.
#
# Multi-stage build:
#   stage 1 "build"  : full Go toolchain (~900 MB) compiles two static binaries
#   stage 2 (runtime): small Alpine image that only receives the two binaries (~195 MB,
#                      almost all of it the MinIO and mc executables themselves)
# The final image never contains the compiler, source code or module cache.

# Base images pinned to exact tags so a rebuild next month gives the same result.
ARG GO_IMAGE=golang:1.26.11-bookworm
ARG RUNTIME_IMAGE=alpine:3.22.8

# ---------------------------------------------------------------- stage 1: build
FROM ${GO_IMAGE} AS build
# bash instead of dash: needed for ${VAR:0:12} below; pipefail makes pipes fail loudly.
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Release tag AND the commit it should point at. Tags can be moved; commits can't.
# For annotated tags use the peeled commit: `git ls-remote <repo> "refs/tags/<tag>^{}"`.
ARG MINIO_TAG=RELEASE.2025-10-15T17-29-55Z
ARG MINIO_SHA=9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a
ARG MINIO_VERSION=2025-10-15T17:29:55Z
ARG MC_TAG=RELEASE.2025-08-13T08-35-41Z
ARG MC_SHA=7394ce0dd2a80935aded936b09fa12cbb3cb8096

# CGO_ENABLED=0 -> fully static binaries that run on Alpine (musl) without glibc.
# GOTOOLCHAIN=local -> use the Go in this image, never download a different one.
# -trimpath -> strip local paths from the binary (reproducible builds).
ENV CGO_ENABLED=0 GOTOOLCHAIN=local GOFLAGS=-trimpath

WORKDIR /src
RUN git clone --quiet --depth 1 --branch "${MINIO_TAG}" https://github.com/minio/minio.git minio \
 && test "$(git -C minio rev-parse HEAD)" = "${MINIO_SHA}" \
 && git clone --quiet --depth 1 --branch "${MC_TAG}" https://github.com/minio/mc.git mc \
 && test "$(git -C mc rev-parse HEAD)" = "${MC_SHA}"

# Cache mounts keep downloaded modules and compiled packages between builds on this
# machine, without baking them into any image layer.
# -ldflags -s -w drops debug symbols (smaller); -X stamps the version the binary reports.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    cd minio && go build -o /out/minio -ldflags "-s -w \
      -X github.com/minio/minio/cmd.Version=${MINIO_VERSION} \
      -X github.com/minio/minio/cmd.ReleaseTag=${MINIO_TAG} \
      -X github.com/minio/minio/cmd.CommitID=${MINIO_SHA} \
      -X github.com/minio/minio/cmd.ShortCommitID=${MINIO_SHA:0:12}" .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    cd mc && go build -o /out/mc -ldflags "-s -w \
      -X github.com/minio/mc/cmd.ReleaseTag=${MC_TAG} \
      -X github.com/minio/mc/cmd.CommitID=${MC_SHA}" .

# ---------------------------------------------------------------- stage 2: runtime
FROM ${RUNTIME_IMAGE}

# ca-certificates for any outbound TLS; a dedicated non-root user owns the data dir.
RUN apk add --no-cache ca-certificates \
 && addgroup -S minio && adduser -S -G minio -H -h /data minio \
 && mkdir -p /data && chown minio:minio /data

COPY --from=build /out/minio /out/mc /usr/local/bin/

USER minio
# mc keeps its aliases in a config dir; point it somewhere writable.
ENV MC_CONFIG_DIR=/tmp/.mc
EXPOSE 9000 9001
VOLUME ["/data"]

# Busybox wget ships with Alpine, so no extra package is needed for the health check.
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=5 \
  CMD wget -q -O /dev/null http://127.0.0.1:9000/minio/health/live || exit 1

ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]
