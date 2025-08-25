#!/usr/bin/env bash
set -Eeuo pipefail

# Optional Docker Hub login (only if creds provided)
if [[ -n "${DOCKERHUB_USERNAME:-}" && -n "${DOCKERHUB_TOKEN:-}" ]]; then
  echo "$DOCKERHUB_TOKEN" | docker login -u "$DOCKERHUB_USERNAME" --password-stdin
fi

# Login to GHCR (needed for ghcr.io pushes)
if [[ -n "${GITHUB_TOKEN:-}" ]]; then
  echo "$GITHUB_TOKEN" | docker login ghcr.io -u "${GITHUB_USERNAME:-github-actions}" --password-stdin
fi

# Build & push
docker build -t "${CONTAINER_RELEASE_IMAGE}" -f Dockerfile .
docker push "${CONTAINER_RELEASE_IMAGE}"
