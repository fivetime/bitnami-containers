#!/usr/bin/env bash
# Smoke-test an InfluxDB 2.x image: version, first-run setup, health, write and query.
#
# Usage: smoke-test.sh <image> <expected-version>
set -euo pipefail

IMAGE="${1:?usage: smoke-test.sh <image> <expected-version>}"
VERSION="${2:?usage: smoke-test.sh <image> <expected-version>}"
NAME="influxdb2-smoke-$$"
TOKEN="smoke-test-token"
ORG="smoke"
BUCKET="smoke"

cleanup() {
    if [[ $? -ne 0 ]]; then
        echo "--- container logs ---" >&2
        docker logs "$NAME" >&2 || true
    fi
    docker rm -f "$NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "checking influxd version"
docker run --rm "$IMAGE" influxd version | tee /dev/stderr | grep -q "InfluxDB v${VERSION} "

echo "starting container with automated setup"
docker run -d --name "$NAME" \
    -e DOCKER_INFLUXDB_INIT_MODE=setup \
    -e DOCKER_INFLUXDB_INIT_USERNAME=admin \
    -e DOCKER_INFLUXDB_INIT_PASSWORD=smoke-test-password \
    -e DOCKER_INFLUXDB_INIT_ORG="$ORG" \
    -e DOCKER_INFLUXDB_INIT_BUCKET="$BUCKET" \
    -e DOCKER_INFLUXDB_INIT_ADMIN_TOKEN="$TOKEN" \
    "$IMAGE" >/dev/null

# Setup runs a temporary influxd first; 8086 only answers once the real one is up.
for _ in $(seq 1 60); do
    if docker exec "$NAME" curl -fsS http://localhost:8086/health 2>/dev/null | grep -q '"status": *"pass"'; then
        break
    fi
    if [[ "$(docker inspect -f '{{.State.Running}}' "$NAME")" != "true" ]]; then
        echo "container exited during startup" >&2
        exit 1
    fi
    sleep 2
done
docker exec "$NAME" curl -fsS http://localhost:8086/health | grep -q '"status": *"pass"'
echo "health: pass"

echo "writing and querying a point"
docker exec "$NAME" influx write -t "$TOKEN" -o "$ORG" -b "$BUCKET" 'smoke,host=ci value=42'
docker exec "$NAME" influx query -t "$TOKEN" -o "$ORG" --raw \
    "from(bucket: \"$BUCKET\") |> range(start: -1h) |> filter(fn: (r) => r._measurement == \"smoke\")" |
    grep -q ',42,'

echo "smoke test passed for $IMAGE"
