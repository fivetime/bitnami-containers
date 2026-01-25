# Copyright The Rook Authors.
# SPDX-License-Identifier: Apache-2.0

# Ceph Consumer Controller Image
# Lightweight image with only kubectl and Python for K8s resource sync

FROM python:3.12-alpine

LABEL maintainer="Rook Community <rook-dev@googlegroups.com>"
LABEL org.opencontainers.image.title="Ceph Consumer Controller"
LABEL org.opencontainers.image.description="Sync controller for connecting Kubernetes clusters to external Rook-Ceph storage"
LABEL org.opencontainers.image.source="https://github.com/rook/rook"
LABEL org.opencontainers.image.licenses="Apache-2.0"

ARG KUBECTL_VERSION=v1.35.0
ARG TARGETARCH=amd64

# Install kubectl and Python dependencies
RUN apk add --no-cache curl ca-certificates \
    && curl -fsSL "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${TARGETARCH}/kubectl" \
        -o /usr/local/bin/kubectl \
    && chmod +x /usr/local/bin/kubectl \
    && kubectl version --client \
    && pip install --no-cache-dir pyyaml \
    && apk del curl \
    && rm -rf /var/cache/apk/*

# Create non-root user
RUN addgroup -g 65532 -S nonroot \
    && adduser -u 65532 -S -G nonroot -h /app nonroot

# Create app directory
WORKDIR /app

# Copy sync script
COPY --chown=nonroot:nonroot scripts/sync.py /app/sync.py
RUN chmod +x /app/sync.py

# Switch to non-root user
USER 65532:65532

# Default entrypoint
ENTRYPOINT ["python3", "/app/sync.py"]
