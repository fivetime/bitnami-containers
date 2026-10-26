# InfluxDB 2.x 镜像（从源码编译）

`ghcr.io/fivetime/influxdb` 是 InfluxDB 2.x OSS 的 Alpine 镜像。镜像里的 `influxd`
由 [influxdata/influxdb](https://github.com/influxdata/influxdb/tags) 的 **v2.x tag 源码编译**而来，
不依赖 InfluxData 发布二进制，也不用等 Docker Hub 的官方镜像。

运行时布局与官方 `influxdb:2.x-alpine` 完全相同，可以直接替换：

- 入口脚本 `/entrypoint.sh`、默认配置 `/etc/defaults/influxdb2/config.yml` 原样取自
  [influxdata-docker](https://github.com/influxdata/influxdata-docker) 的 `influxdb/2.9/alpine`
  （提交 `c22862a6fae0c5f642fd5461946fee46e387e821`），所以 `DOCKER_INFLUXDB_INIT_*` 初始化变量、
  `/docker-entrypoint-initdb.d` 初始化脚本、`INFLUXD_*` 配置变量的行为都和官方一致；
- 数据目录 `/var/lib/influxdb2`、配置目录 `/etc/influxdb2`，以 `influxdb` 用户（uid/gid 1000）运行；
- 端口 8086。

## 镜像 tag

| tag | 含义 |
|---|---|
| `X.Y.Z`（如 `2.10.0`） | 由源码 tag `vX.Y.Z` 编译 |
| `latest` | **永远指向最新的 v2.x tag**（只认正式版 `vX.Y.Z`，不含 rc） |

镜像同时提供 `linux/amd64` 和 `linux/arm64` 两个架构。

生产部署请用具体版本号，最好再钉 digest，不要用 `latest`：
InfluxDB 启动时会自动迁移元数据，`latest` 一变就等于静默升级了数据库。

## 构建方式

`Dockerfile` 分两个阶段：

1. **编译阶段**（`golang:1.26-alpine3.23`）：浅克隆 tag `v${INFLUXDB_VERSION}`，按该 tag 里官方发版用的
   `scripts/ci/build.sh` 编译，得到与官方 Linux 二进制同类的**静态 PIE**（musl，不依赖任何动态库）：
   - `make generate-web-assets`：从 [influxdata/ui](https://github.com/influxdata/ui/releases) 下载该 tag 指定的 UI 资源，
     按其 `sha256.txt` 校验，连同 swagger 文档一起编进二进制；
   - Flux 引擎 libflux 是 Rust 库，由源码自带的 `scripts/pkg-config.sh`（influxdata/pkg-config）调用 cargo 编译，再经 cgo 链接。
     `static_build` 模式下它要求 Rust 目标 `x86_64-unknown-linux-musl` / `aarch64-unknown-linux-musl`，
     Alpine 自带的 rust 包是 `*-alpine-linux-musl`，对不上，所以用 rustup。在 Alpine 上 rustup 工具链的宿主正是上述目标，
     无需交叉编译器。Rust 版本不由本镜像决定：Flux 模块在 `rust-toolchain.toml` 里钉了自己的工具链
     （flux v0.201.0 是 1.83），rustup 在编 libflux 时按它自动安装；
   - build tag 与官方一致：`assets,sqlite_foreign_keys,sqlite_json,flux_nosqlite,static_build,noasm`；
     链接参数 `-buildmode=pie -linkmode=external -extldflags '-fno-PIC -static-pie -Wl,-z,stack-size=8388608 … -lunwind'`，
     其中 libunwind 取自编 libflux 的那个 Rust 工具链里 musl 目标自带的 self-contained 库
     （即官方构建里 `find_rustlib_path` 指向的目录）；
   - musl 1.2.4 起去掉了 `pread64`、`off64_t` 等 LFS64 名字，go-sqlite3 自带的 sqlite 还在用，
     因此加 `CGO_CFLAGS=-D_LARGEFILE64_SOURCE` 让它们以宏的形式回来；
   - Go 版本按该 tag `go.mod` 的要求自动获取（`GOTOOLCHAIN=auto`）；
   - 版本号写入 `v${INFLUXDB_VERSION}`，commit 取 10 位缩写，`influxd version` 的输出格式与官方二进制一致。
2. **运行阶段**（`alpine:3.23`）：沿用官方镜像的软件包、dasel、用户和目录，只把 `influxd` 换成上面编出来的。
   `influx` CLI 仍用 InfluxData 发布的 2.8.0 二进制，GPG 验签后装入。

### 本地构建

```bash
docker build --build-arg INFLUXDB_VERSION=2.10.0 -t influxdb:2.10.0 bitnami/influxdb/2
```

`INFLUXDB_VERSION` 必须对应一个已存在的 tag `vX.Y.Z`，不写时默认用 Dockerfile 里的值。
编译阶段要编 Rust 和 Go，很吃 CPU。在跑着其他服务的机器上构建时请限制核数，例如：

```bash
docker buildx create --name limited --driver docker-container \
  --driver-opt cpu-quota=400000 --driver-opt cpu-period=100000 --driver-opt memory=8g
docker buildx build --builder limited --load \
  --build-arg INFLUXDB_VERSION=2.10.0 -t influxdb:2.10.0 bitnami/influxdb/2
```

### 自动构建（`.github/workflows/influxdb-2.yml`）

- **每天定时**查一次 influxdata/influxdb 最新的 `v2.x.y` tag，ghcr 上还没有这个版本就编译并推送，同时更新 `latest`；
- **改动本目录或工作流**推到 main 时，重新编译最新 tag；
- **手动触发**可以指定 `version` 编译旧版本。旧版本只打自己的版本号 tag，不动 `latest`；
- 两个架构分别在 GitHub 原生运行器（`ubuntu-24.04` / `ubuntu-24.04-arm`）上编译，再合并成一个多架构 manifest。

## 使用

和官方镜像用法一样，例如：

```bash
docker run -d -p 8086:8086 \
  -v influxdb2-data:/var/lib/influxdb2 \
  -v influxdb2-config:/etc/influxdb2 \
  -e DOCKER_INFLUXDB_INIT_MODE=setup \
  -e DOCKER_INFLUXDB_INIT_USERNAME=admin \
  -e DOCKER_INFLUXDB_INIT_PASSWORD=<password> \
  -e DOCKER_INFLUXDB_INIT_ORG=<org> \
  -e DOCKER_INFLUXDB_INIT_BUCKET=<bucket> \
  ghcr.io/fivetime/influxdb:2.10.0
```

在 influxdata 官方 Helm chart `influxdb2` 里只需覆盖镜像：

```yaml
image:
  repository: ghcr.io/fivetime/influxdb
  tag: "2.10.0"
```

## 从 2.7 及更早版本升级要注意

- **2.8.0** 起 BoltDB 新增一个索引 bucket。只要从没开启过 token 哈希，仍然可以用 `influxd downgrade` 退回 2.7.x。
- **2.9.0** 起 **API token 默认哈希存储**：启动时就把已有 token 哈希掉，明文再也取不回来；
  一旦开启过，再退回 2.8.0 以前的版本会清空所有 token。
  升级前先把还要用的 token（包括 operator token）的明文另存一份。想保留回退能力的话，
  先用 `INFLUXD_USE_HASHED_TOKENS=false` 升级，确认没问题后再单独开启哈希。
- **2.10.0** 新增 `/health`、`/ready` 的认证选项 `--health-auth-mode`。默认只有开了 `--hardening-enabled`
  才需要认证，所以不带凭据的存活/就绪探针照常可用。另外新增 `--influxql-max-time-range`，默认 0 即不限制。
- 每次元数据迁移前，`influxd` 会把 bolt 文件自动备份成 `influxd.bolt.pre-vX.Y.Z-upgrade.backup`。
  数据较多时，升级前仍建议先做一次 `influx backup`。

## 许可

InfluxDB 2.x 源码采用 MIT 许可（见各 v2.x tag 里的 `LICENSE`）。运行阶段的 `entrypoint.sh`、
`default-config.yml` 来自 influxdata-docker，该仓库没有附许可证文件。
