# InfluxDB 2.x (official Dockerfile, auto-built)

`ghcr.io/fivetime/influxdb` follows the latest stable InfluxDB 2.x release. It is built
with the Dockerfile behind the official `docker.io/library/influxdb` image
([influxdata/influxdata-docker](https://github.com/influxdata/influxdata-docker)),
so it can be used before Docker Hub publishes a new 2.x tag. Bitnami stopped
shipping InfluxDB 2.x at 2.7.11.

## How it works

`.github/workflows/influxdb2.yml` runs daily, on changes to this directory, and on demand:

1. `prepare.sh` finds the latest stable `v2.x.y` tag of `influxdata/influxdb`, checks
   that its amd64 and arm64 binaries and signatures are on `dl.influxdata.com`, and
   copies the matching `influxdb/2.x` directory from `influxdata-docker` (or the newest
   one, if the new minor has no directory yet) with `INFLUXDB_VERSION` set.
2. The run stops if that image tag already exists.
3. `smoke-test.sh` checks the version, runs first-time setup, waits for `/health`, and
   writes and queries a point. Nothing is pushed if it fails.
4. The image is built for amd64 and arm64 and pushed as `X.Y.Z`, `X.Y` and `2`.

Pull requests build and test without pushing.

## Manual runs

```sh
gh workflow run influxdb2.yml -R fivetime/bitnami-containers                    # latest
gh workflow run influxdb2.yml -R fivetime/bitnami-containers -f version=2.9.1   # pinned
gh workflow run influxdb2.yml -R fivetime/bitnami-containers -f force=true      # rebuild
```

A pinned older version only gets its `X.Y.Z` tag; `X.Y` and `2` always follow the
latest release.

## Local build

```sh
./prepare.sh /tmp/influxdb2-context
docker build -t influxdb2-test /tmp/influxdb2-context
./smoke-test.sh influxdb2-test 2.10.0
```
