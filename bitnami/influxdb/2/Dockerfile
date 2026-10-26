# InfluxDB 2.x OSS on Alpine, compiled from a v2.x tag of
# github.com/influxdata/influxdb.
#
# A drop-in for the official influxdb:2.x-alpine image: same entrypoint,
# paths, uid 1000 and DOCKER_INFLUXDB_INIT_* variables. The runtime stage is
# github.com/influxdata/influxdata-docker influxdb/2.9/alpine at
# c22862a6fae0c5f642fd5461946fee46e387e821 (entrypoint.sh and
# default-config.yml unchanged), with influxd built here instead of
# downloaded.
#
#   docker build --build-arg INFLUXDB_VERSION=2.10.0 .

ARG INFLUXDB_VERSION=2.10.0

# influxd is built the way the official release binaries are
# (scripts/ci/build.sh in the tag): a static PIE against musl, with libflux
# compiled for the *-unknown-linux-musl Rust target. On Alpine that target is
# the host, so no cross compiler is needed.
FROM golang:1.26-alpine3.23 AS build

ARG INFLUXDB_VERSION

# influxdata/pkg-config (scripts/pkg-config.sh) runs cargo to build libflux,
# the Flux engine (Rust, linked through cgo), and hands everything else to
# the real pkg-config (pkgconf). coreutils: fetch-ui-assets.sh checks the UI
# with sha256sum --check, which BusyBox's sha256sum does not take.
RUN apk add --no-cache \
      bash \
      build-base \
      coreutils \
      curl \
      git \
      pkgconf \
      rustup

# Alpine's own rust package is built for x86_64-alpine-linux-musl, but with
# static_build pkg-config asks cargo for x86_64-unknown-linux-musl (and its
# aarch64 counterpart); rustup's toolchains on Alpine have exactly that as
# host. No default toolchain: the Flux module pins its own in
# rust-toolchain.toml (1.83 for flux v0.201.0), and rustup installs it when
# cargo runs there.
ENV PATH=/root/.cargo/bin:$PATH
RUN rustup-init -y --profile minimal --default-toolchain none --no-modify-path

# A tag asks for its own Go release in go.mod; let go fetch it.
ENV GOTOOLCHAIN=auto
# musl 1.2.4 dropped pread64, off64_t and the other LFS64 names; the sqlite
# that go-sqlite3 bundles uses them, and gets them back as macros with
# _LARGEFILE64_SOURCE. -O2 -g is cgo's default, kept.
ENV CGO_CFLAGS="-O2 -g -D_LARGEFILE64_SOURCE"

RUN git clone --depth 1 --branch "v${INFLUXDB_VERSION}" \
      https://github.com/influxdata/influxdb.git /src/influxdb

WORKDIR /src/influxdb

# The UI assets come from the influxdata/ui release the tag names, checked
# against its sha256.txt, and are compiled in with the swagger document.
RUN make generate-web-assets

# Tags, link mode and flags as in the tag's scripts/ci/build.sh. libunwind
# for the Rust objects is the one the Rust musl target ships (what the
# release build's find_rustlib_path points at), taken from the toolchain the
# Flux module pins, the one that compiles libflux. The commit is abbreviated
# to 10 characters like the official binaries (a shallow clone would give 7).
RUN flux="$(go mod download -json github.com/influxdata/flux | sed -n 's/^[[:space:]]*"Dir": "\(.*\)",$/\1/p')" && \
    test -n "$flux" && \
    rustlib="$(cd "$flux" && echo "$(rustc --print sysroot)/lib/rustlib/$(rustc -vV | sed -n 's/^host: //p')/lib/self-contained")" && \
    test -f "$rustlib/libunwind.a" && \
    commit="$(git rev-parse --short=10 HEAD)" && \
    build_date="$(date -u +'%Y-%m-%dT%H:%M:%SZ')" && \
    CGO_ENABLED=1 PKG_CONFIG="$PWD/scripts/pkg-config.sh" go build \
      -tags assets,sqlite_foreign_keys,sqlite_json,flux_nosqlite,static_build,noasm \
      -buildmode=pie \
      -ldflags "-s -w -X main.version=v${INFLUXDB_VERSION} -X main.commit=${commit} -X main.date=${build_date} -linkmode=external -extldflags '-fno-PIC -static-pie -Wl,-z,stack-size=8388608 -L${rustlib} -Wl,-lunwind'" \
      -o /usr/local/bin/influxd \
      ./cmd/influxd && \
    influxd version

FROM alpine:3.23

ARG INFLUXDB_VERSION

RUN echo 'hosts: files dns' >> /etc/nsswitch.conf
RUN apk add --no-cache \
      bash \
      ca-certificates \
      curl \
      gnupg \
      run-parts \
      setpriv \
      tzdata && \
    update-ca-certificates

# Install dasel for configuration parsing
RUN case "$(apk --print-arch)" in \
      x86_64)  arch=amd64 ;; \
      aarch64) arch=arm64 ;; \
      *) echo 'Unsupported architecture' && exit 1 ;; \
    esac && \
    curl -fL "https://github.com/TomWright/dasel/releases/download/v3.4.1/dasel_linux_${arch}.gz" | gzip -d > /usr/local/bin/dasel && \
    case ${arch} in \
      amd64) echo '3c947a8dcd88856a32c172081db091c38059394fb57a15fa43871f6d046427e1  /usr/local/bin/dasel' ;; \
      arm64) echo 'a128c5554c53e6e4af880700adba1d212ce651db208da1592fb1cae0e959cbc6  /usr/local/bin/dasel' ;; \
    esac | sha256sum -c - && \
    chmod +x /usr/local/bin/dasel && \
    dasel version

RUN addgroup -S -g 1000 influxdb && \
    adduser -S -G influxdb -u 1000 -h /home/influxdb -s /bin/sh influxdb && \
    mkdir -p /home/influxdb && \
    chown -R influxdb:influxdb /home/influxdb

# The influxd server, compiled above
ENV INFLUXDB_VERSION=${INFLUXDB_VERSION}
COPY --from=build /usr/local/bin/influxd /usr/local/bin/influxd
RUN influxd version

# Install the influx CLI
ENV INFLUX_CLI_VERSION=2.8.0
RUN case "$(apk --print-arch)" in \
      x86_64)  arch=amd64 ;; \
      aarch64) arch=arm64 ;; \
      *) echo 'Unsupported architecture' && exit 1 ;; \
    esac && \
    export GNUPGHOME="$(mktemp -d)" && \
    gpg --batch --keyserver keyserver.ubuntu.com --recv-keys \
      # InfluxData Package Signing Key <support@influxdata.com>
      24C975CBA61A024EE1B631787C3D57159FC2F927 && \
    curl -fLO "https://dl.influxdata.com/influxdb/releases/influxdb2-client-${INFLUX_CLI_VERSION}-linux-${arch}.tar.gz" \
         -fLO "https://dl.influxdata.com/influxdb/releases/influxdb2-client-${INFLUX_CLI_VERSION}-linux-${arch}.tar.gz.asc" && \
    gpg --batch --verify "influxdb2-client-${INFLUX_CLI_VERSION}-linux-${arch}.tar.gz.asc" \
                         "influxdb2-client-${INFLUX_CLI_VERSION}-linux-${arch}.tar.gz" && \
    tar xzf "influxdb2-client-${INFLUX_CLI_VERSION}-linux-${arch}.tar.gz" -C /usr/local/bin ./influx && \
    rm -rf "influxdb2-client-${INFLUX_CLI_VERSION}-linux-${arch}.tar.gz" \
           "influxdb2-client-${INFLUX_CLI_VERSION}-linux-${arch}.tar.gz.asc" && \
    influx version

# Create standard directories expected by the entry-point.
RUN mkdir /docker-entrypoint-initdb.d && \
    mkdir -p /var/lib/influxdb2 && \
    chown -R influxdb:influxdb /var/lib/influxdb2 && \
    mkdir -p /etc/influxdb2 && \
    chown -R influxdb:influxdb /etc/influxdb2
VOLUME /var/lib/influxdb2 /etc/influxdb2

COPY default-config.yml /etc/defaults/influxdb2/config.yml
COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["influxd"]

EXPOSE 8086

ENV INFLUX_CONFIGS_PATH=/etc/influxdb2/influx-configs
ENV INFLUXD_INIT_PORT=9999
ENV INFLUXD_INIT_PING_ATTEMPTS=600
ENV DOCKER_INFLUXDB_INIT_CLI_CONFIG_NAME=default
