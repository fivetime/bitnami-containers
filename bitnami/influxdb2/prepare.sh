#!/usr/bin/env bash
# Prepare a Docker build context for the latest stable InfluxDB 2.x release,
# using the Dockerfile that builds the official docker.io/library/influxdb image.
#
# Usage: prepare.sh <context-dir> [version]
#   version  Build this version (e.g. 2.10.0) instead of the latest stable v2 tag.
#
# Prints key=value lines on stdout (suitable for $GITHUB_OUTPUT):
#   version    the InfluxDB version to build, e.g. 2.10.0
#   minor      e.g. 2.10
#   latest     true when version is the latest stable 2.x release
#   based_on   the influxdata-docker directory the Dockerfile came from
#   upstream   the influxdata-docker commit used
set -euo pipefail

CONTEXT_DIR="${1:?usage: prepare.sh <context-dir> [version]}"
REQUESTED="${2:-}"

INFLUXDB_REPO="https://github.com/influxdata/influxdb.git"
DOCKER_REPO="https://github.com/influxdata/influxdata-docker.git"
DL_BASE="https://dl.influxdata.com/influxdb/releases"
ARCHES=(amd64 arm64)

log() { echo "$*" >&2; }

# Latest stable v2.x.y tag; pre-releases (rc, beta) never match the pattern.
latest="$(git ls-remote --tags --refs "$INFLUXDB_REPO" 'v2.*' |
    awk '{sub("refs/tags/v", "", $2); print $2}' |
    grep -E '^2\.[0-9]+\.[0-9]+$' | sort -V | tail -1)"
[[ -n "$latest" ]] || { log "no stable v2 tag found"; exit 1; }

version="${REQUESTED:-$latest}"
[[ "$version" =~ ^2\.[0-9]+\.[0-9]+$ ]] || { log "invalid version: $version"; exit 1; }
minor="${version%.*}"
log "latest stable 2.x: $latest, building: $version"

# A tag can exist days before its binaries are published.
for arch in "${ARCHES[@]}"; do
    for suffix in "" ".asc"; do
        url="$DL_BASE/influxdb2-${version}_linux_${arch}.tar.gz${suffix}"
        if ! curl -fsSI -o /dev/null "$url"; then
            log "not published yet: $url"
            exit 3
        fi
    done
done

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
git clone -q --depth 1 "$DOCKER_REPO" "$work/influxdata-docker"
upstream="$(git -C "$work/influxdata-docker" rev-parse --short=10 HEAD)"

# Prefer the directory for this exact minor; otherwise fall back to the newest 2.x one.
if [[ -d "$work/influxdata-docker/influxdb/$minor" ]]; then
    based_on="$minor"
else
    based_on="$(find "$work/influxdata-docker/influxdb" -mindepth 1 -maxdepth 1 -type d -printf '%f\n' |
        grep -E '^2\.[0-9]+$' | sort -V | tail -1)"
fi
[[ -n "$based_on" ]] || { log "no influxdb/2.x directory in influxdata-docker"; exit 1; }
log "using influxdata-docker influxdb/$based_on @ $upstream"

rm -rf "$CONTEXT_DIR"
mkdir -p "$CONTEXT_DIR"
cp -a "$work/influxdata-docker/influxdb/$based_on/." "$CONTEXT_DIR/"
rm -rf "$CONTEXT_DIR/alpine"
sed -i -E "s/^ENV INFLUXDB_VERSION=.*/ENV INFLUXDB_VERSION=${version}/" "$CONTEXT_DIR/Dockerfile"
grep -qx "ENV INFLUXDB_VERSION=${version}" "$CONTEXT_DIR/Dockerfile" ||
    { log "failed to set INFLUXDB_VERSION in Dockerfile"; exit 1; }

echo "version=$version"
echo "minor=$minor"
echo "latest=$([[ "$version" == "$latest" ]] && echo true || echo false)"
echo "based_on=$based_on"
echo "upstream=$upstream"
