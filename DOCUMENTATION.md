# Warp — S3 Benchmark Tool

Warp is a high-performance benchmarking tool for S3-compatible object storage systems. It supports multiple benchmark types, distributed execution across machines, real-time monitoring, and detailed post-run analysis.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [CLI Commands](#cli-commands)
3. [Benchmark Types](#benchmark-types)
4. [Distributed Mode (Client/Server)](#distributed-mode-clientserver)
5. [Configuration](#configuration)
6. [Object Generation](#object-generation)
7. [Output Data Format](#output-data-format)
8. [Analysis & Aggregation](#analysis--aggregation)
9. [Auto-Termination](#auto-termination)
10. [YAML Configuration](#yaml-configuration)
11. [Monitoring & Integration](#monitoring--integration)
12. [Build & Release](#build--release)

---

## Architecture Overview

```
main.go
  └─ cli.Main()
       ├─ registerApp() → builds CLI app with all commands
       ├─ Bench Commands (get, put, delete, mixed, ...)
       ├─ Utility Commands (analyze, cmp, merge, client, run)
       └─ Distributed Mode (warp-client → server → clients)
```

### Package Dependency Flow

```
pkg/generator/  ← zero warp dependencies (test data generation)
       ↑
pkg/bench/      ← benchmarks, operations, collection
       ↑
pkg/aggregate/  ← analysis, statistics, live aggregation
       ↑
cli/            ← CLI layer (commands, flags, UI)
api/ wui/       ← HTTP API, web UI
```

### Layered Dependencies

| Package | Imports | Purpose |
|---|---|---|
| `pkg/generator` | none (stdlib + prng) | Random data + object generation |
| `pkg/bench` | `pkg/generator` | Benchmark interface, operations, CSV I/O |
| `pkg/aggregate` | `pkg/bench` | Aggregation, throughput calculations, percentiles |
| `cli/` | `pkg/bench`, `pkg/aggregate` | CLI commands, flags, distributed mode, UI |
| `api/` | `pkg/bench`, `pkg/aggregate` | HTTP API for monitoring running benchmarks |
| `wui/` | `pkg/aggregate` | Web UI with real-time charts |

---

## CLI Commands

### Entry Point

`main.go` delegates to `cli.Main()`. The app registers all commands in `cli/cli.go:init()`:

```
Benchmark Commands:
  mixed       - mixed get/put workload (default benchmark suite)
  get         - GET object benchmark
  put         - PUT object benchmark
  delete      - DELETE object benchmark
  list        - LIST objects benchmark
  stat        - STAT object benchmark
  versioned   - versioned bucket benchmark
  retention   - object retention benchmark
  multipart   - multipart upload benchmark
  multipart-put - multipart PUT benchmark
  zip         - S3 Select / Zip benchmark
  snowball    - Snowball TAR benchmark
  fanout      - fan-out benchmark (e.g., Iceberg manifest reads)
  append      - Append Object benchmark
  iceberg     - Iceberg REST catalog benchmarks (subcommands)

Utility Commands:
  analyze     - analyze benchmark data from .csv.zst
  cmp         - compare two benchmark runs
  merge       - merge results from multiple clients
  client      - run warp in client mode (distributed)
  run         - run benchmark from YAML file
```

### Common Flags

All benchmark commands share these flags (defined in `cli/flags.go` and `cli/benchmark.go`):

| Flag | Type | Default | Description |
|---|---|---|---|
| `--host` | string | `127.0.0.1:9000` | S3 endpoint (comma-separated, `file:` prefix, `{0...N}` expansion) |
| `--access-key` | string | `""` | S3 access key (env: `WARP_ACCESS_KEY`) |
| `--secret-key` | string | `""` | S3 secret key (env: `WARP_SECRET_KEY`) |
| `--tls` | bool | false | Use HTTPS |
| `--ktls` | bool | false | Use Kernel TLS |
| `--region` | string | `""` | S3 region (env: `WARP_REGION`) |
| `--bucket` | string | `warp-benchmark-bucket` | Bucket name |
| `--concurrent` | int | `auto` | Number of concurrent operations |
| `--duration` | duration | `5m` | Benchmark duration |
| `--obj.size` | string | `10MiB` | Object size (fixed, random, bucketed) |
| `--obj.randsize` | bool | false | Randomize object sizes |
| `--autoterm` | bool | false | Auto-terminate when stable |
| `--warp-client` | string | `""` | Distributed client list |
| `--noclear` | bool | false | Skip bucket cleanup (required for multi-client) |
| `--quiet` | bool | false | Suppress progress bar |
| `--json` | bool | false | JSON output |
| `--no-color` | bool | false | Disable color output |
| `--debug` | bool | false | Debug logging |

### Host Expansion Syntax

The `--host` and `--warp-client` flags support multiple input formats:

1. **Comma-separated**: `host1:9000,host2:9000,host3:9000`
2. **Ellipsis expansion**: `minio-{0...3}.minio.local:9000` → 4 hosts
3. **File prefix**: `file:hosts.txt` (one host:port per line, supports ellipsis within file)

---

## Benchmark Types

### GET Benchmark (`get`)
Downloads existing objects. Requires `--preloaded` objects or a pre-populated bucket.

### PUT Benchmark (`put`)
Uploads generated random data using the generator package. Creates objects during preparation and benchmarks the upload throughput.

### DELETE Benchmark (`delete`)
Deletes existing objects. Requires pre-uploaded objects.

### LIST Benchmark (`list`)
Lists objects with configurable prefixes and pagination.

### STAT Benchmark (`stat`)
Performs HEAD/STAT operations on existing objects.

### MIXED Benchmark (`mixed`)
Runs a mix of GET and PUT operations (default), configurable via `--get-distrib` and `--put-distrib`.

### Versioned Benchmark (`versioned`)
Tests versioned bucket operations (PUT, DELETE with versioning enabled).

### Retention Benchmark (`retention`)
Tests object lock and retention policy operations.

### Multipart Benchmark (`multipart`)
Tests multipart upload lifecycle operations.

### Zip Benchmark (`zip`)
Tests S3 Select / Zip archive operations.

### Snowball Benchmark (`snowball`)
Tests Snowball TAR archive upload operations.

### Fan-out Benchmark (`fanout`)
Tests concurrent reads from multiple objects (used for Iceberg manifest reads).

### Append Benchmark (`append`)
Tests Append Object operations.

### Iceberg Benchmarks (`iceberg`)
Suite of benchmarks for Iceberg REST catalog:
- `iceberg catalog-commits` — commit operations
- `iceberg catalog-mixed` — mixed catalog workload
- `iceberg catalog-read` — read-only catalog workload
- `iceberg sustained` — sustained throughput test

---

## Distributed Mode (Client/Server)

Warp supports distributed execution where a server orchestrates benchmark execution across multiple warp client instances. This is how the multi-client mechanism works:

### Architecture

```
┌─────────────────────────┐
│   Server (orchestrator) │  ← runs warp get --warp-client=...
│   WebSocket client      │
└──────┬──────┬──────┬────┘
       │      │      │
       ▼      ▼      ▼
┌────────┐┌────────┐┌────────┐
│Client 0││Client 1││Client 2│  ← each runs warp client :7761
└────────┘└────────┘└────────┘
```

### Protocol

The distributed mode uses **WebSocket** (port 7761 by default):

1. **Server connects** to each client via `ws://host:7761/ws`
2. **Handshake**: Server sends `serverInfo` (ID, version), client validates
3. **Benchmark request**: Server sends `serverRequest` with serialized CLI flags, command name, and args
4. **Stage coordination**: Server orchestrates phases via `start_stage` / `stage_status` messages:
   - `prepare` — clients create buckets, upload initial objects
   - `benchmark` — clients execute concurrent operations
   - `cleanup` — clients remove test data
5. **Result collection**: Server downloads operations via `send_ops` (full CSV data or aggregated JSON)

### Key Implementation Files

| File | Purpose |
|---|---|
| `cli/benchserver.go` | Server-side orchestration (`runServerBenchmark`, `connections`, `roundTrip`) |
| `cli/benchclient.go` | Client-side execution (`serveWs`, `executeBenchmark`, `runCommand`) |
| `cli/clientmode.go` | `warp client` command, listens on `:7761` for WebSocket connections |
| `cli/benchmark.go` | `runClientBenchmark` — benchmark runner for distributed clients |

### Server Flow (`runServerBenchmark`)

```
1. Parse --warp-client hosts
2. Connect to each client via WebSocket
3. Send benchmark config (flags, command, args)
4. Stage: Prepare → wait for all clients
5. Stage: Benchmark → wait for completion (poll status every 1s)
   - Auto-term support with real-time updates
6. Download operations from all clients
7. Merge results (OffsetThreads for CSV, Merge for aggregated JSON)
8. Stage: Cleanup (unless --keep-data or --noclear)
9. Print analysis
```

### Client Flow (`serveWs`)

```
1. Accept WebSocket connection
2. Read serverInfo, validate, respond
3. Loop:
   - receive serverRequest
   - handle by operation type:
     - benchmark: rebuild CLI context, execute benchmark
     - start_stage: trigger stage start
     - stage_status: report progress
     - stage_abort: cancel stage
     - send_ops: return collected operations
     - disconnect: clean up and close
```

### Time Synchronization

The server checks clock synchronization during connect:
- Measures round-trip time
- Verifies client clock is within 1 second of server
- Returns error if delta is too large

### Reconnection

If a WebSocket connection drops, the server retries up to 3 times (with 1s delay between attempts).

---

## Configuration

### CLI Flags

All configuration is done through CLI flags (see [Common Flags](#common-flags)). Environment variables are supported for credentials:

| Variable | Maps To |
|---|---|
| `WARP_HOST` | `--host` |
| `WARP_ACCESS_KEY` | `--access-key` |
| `WARP_SECRET_KEY` | `--secret-key` |
| `WARP_TLS` | `--tls` |
| `WARP_KTLS` | `--ktls` |
| `WARP_REGION` | `--region` |

### YAML Configuration (via `warp run`)

Benchmarks can be fully defined in YAML. See [YAML Configuration](#yaml-configuration) section below.

### InfluxDB Integration

Real-time metrics can be pushed to InfluxDB v2+ using `--influxdb` flag:
```
--influxdb https://token@hostname:8086/bucket/org?tag=key=value
```

The connection string format: `<schema>://<token>@<hostname>:<port>/<bucket>/<org>?<tag=value>`

### TLS Configuration

- **Standard TLS**: `--tls` enables HTTPS.
- **Kernel TLS (kTLS)**: `--ktls` enables in-kernel TLS for maximum performance on Linux.
- **Skip Verify**: `--insecure` disables certificate verification.
- Custom TLS config via `cli/client_tls.go` and `cli/client_ktls.go`.

---

## Object Generation

The `pkg/generator` package handles test data generation. It is a self-contained package with zero warp dependencies.

### Source Interface

```go
type Source interface {
    Object() *Object    // Returns random object data
    String() string     // Human-readable description
    Prefix() string     // Shared prefix
}
```

### Object Structure

```go
type Object struct {
    Reader      LastByter   // io.ReadSeeker with LastByte tracking
    Name        string      // Random name
    ContentType string      // MIME type
    Prefix      string      // Object prefix
    VersionID   string      // S3 version ID
    Size        int64       // Object size
}
```

### Size Modes

1. **Fixed Size**: `--obj.size 10MiB` — all objects identical size
2. **Random Size**: `--obj.randsize --obj.size 1MiB` — objects from 1B to 1MiB (exponential distribution)
3. **Min/Max Size**: `--obj.size 1KiB` `--obj.randsize` — with `--obj.min-size` for lower bound
4. **Bucketed**: `--obj.size 100MiB,1MiB,1KiB` — objects of different fixed sizes

### Random Data

Data is generated using pseudo-random ASCII characters. The generator is deterministic per-source but varies between goroutines. Not suitable for actual randomness/cryptography.

---

## Output Data Format

Benchmark data is saved to compressed files using Zstandard compression (`.csv.zst` or `.json.zst`).

### CSV Format (Full Data)

File pattern: `warp-<operation>-<date>[<time>]-<random>.csv.zst`

Tab-separated fields:

| Field | Type | Description |
|---|---|---|
| `idx` | int | Operation index |
| `thread` | int | Goroutine/thread ID |
| `op` | string | Operation type (`GET`, `PUT`, `DELETE`, `HEAD`, etc.) |
| `client_id` | string | Client identifier |
| `n_objects` | int | Objects per operation |
| `bytes` | int64 | Data transferred |
| `endpoint` | string | S3 endpoint used |
| `file` | string | Object key |
| `error` | string | Error message (empty if success) |
| `start` | RFC3339Nano | Start timestamp |
| `first_byte` | RFC3339Nano | First byte received time |
| `last_byte` | RFC3339Nano | Last byte sent time |
| `end` | RFC3339Nano | End timestamp |
| `duration_ns` | int64 | Duration in nanoseconds |
| `cat` | int | Bitmask categories |

### JSON Format (Aggregated)

File pattern: `warp-<operation>-<date>[<time>]-<random>.json.zst`

Contains a `Realtime` struct with:
- `Total` — aggregate across all operations
- `ByOpType` — per-operation-type breakdown
- `ByHost` — per-endpoint breakdown
- `ByClient` — per-warp-client breakdown
- `ByObjLog2Size` — per-size-range breakdown
- `ByCategory` — per-category breakdown

Each aggregate includes throughput stats, latency percentiles, and time-segmented data.

### Using Output Files

```bash
# Analyze results
warp analyze warp-get-2024-01-01[120000]-abcd.csv.zst

# Compare two runs
warp cmp before.csv.zst after.csv.zst

# Merge results from multiple clients
warp merge client1.csv.zst client2.csv.zst client3.csv.zst
```

---

## Analysis & Aggregation

The `pkg/aggregate` package performs data analysis after benchmarks complete.

### Aggregation Flow

```
Operations (raw)
  │
  ▼
ActiveTimeRange → filter warmup/cooldown
  │
  ▼
Segmentation → split into time segments (~1s each)
  │
  ▼
Stats per segment: throughput, latency, TTFB, errors
  │
  ▼
Full stats: median, 90th/99th percentile, min/max, stddev
  │
  ▼
Report generation → text table with performance summary
  │
  ▼
JSON output → for automated processing
```

### Key Statistics

| Metric | Description |
|---|---|
| **Throughput (MiB/s)** | Data transfer rate |
| **Objects/s** | Operations per second |
| **Latency (avg/p50/p90/p99)** | Request duration distribution |
| **TTFB** | Time to first byte |
| **Errors** | Count and first N error messages |
| **Segmented throughput** | Fastest/median/slowest time segments |

### Analysis Flags

| Flag | Default | Description |
|---|---|---|
| `--analyze.v` | false | Verbose output |
| `--analyze.skip` | `0s` | Skip initial duration |
| `--analyze.dur` | `0s` | Segment duration |
| `--analyze.op` | `""` | Filter by operation type |

---

## Auto-Termination

When `--autoterm` is enabled, warp monitors throughput stability during the benchmark and terminates early if the system has converged.

### How It Works

1. Operations are split into **25 time segments** (configurable)
2. Continuously samples throughput into these segments
3. Checks if the **last 7 segments** are within **7.5%** (configurable) of the current speed
4. Must maintain stability for **15 seconds** (configurable)
5. Prevents premature termination during warmup or unstable periods

### Configuration

```bash
warp get --autoterm \
  --autoterm.dur 15s \
  --autoterm.pct 7.5 \
  --duration 30m  # maximum duration if never stable
```

### Implementation

Auto-term is implemented in `pkg/bench/collector.go` (`AutoTerm` method) and `pkg/aggregate/live.go`:
- Uses `ActiveTimeRange` to find the stable measurement window
- Splits into segments and checks variance
- Returns a cancellable context that triggers when stability is detected

---

## YAML Configuration

Benchmarks can be fully configured via YAML files, useful for complex or repeatable benchmark runs.

### Basic Structure

```yaml
warp:
  api: v1
  benchmark: mixed       # benchmark type name
  remote:                # S3 connection config
    host:
      - minio-{0...3}.minio.local:9000
    access-key: 'minioadmin'
    secret-key: 'minioadmin'
    region: us-east-1
    tls: false
  params:                 # benchmark parameters
    duration: 5m
    concurrent: 16
    objects: 2500
    obj:
      size: 10MiB
```

### Section Mapping

YAML keys map to CLI flags:

| YAML Section | CLI Prefix | Example |
|---|---|---|
| `obj.size` | `--obj.size` | `size: 10MiB` |
| `obj.rand-size` | `--obj.randsize` | `rand-size: true` |
| `analyze.skip-duration` | `--analyze.skip` | `skip-duration: 10s` |
| `autoterm.enabled` | `--autoterm` | `enabled: true` |
| `no-clear` | `--noclear` | `no-clear: true` |
| `remote.host` | `--host` | `host: [...]` |
| `warp-client` | `--warp-client` | `warp-client: [...]` |

### Template Variables

YAML files support Go template substitution:

```yaml
warp:
  api: v1
  benchmark: get
  params:
    duration: {{.Dur}}
    obj:
      size: {{.Size}}
```

Run with:
```bash
warp run config.yml -var Dur=5m -var Size=10MiB
```

### Sample Files

Example YAML files are in `yml-samples/`:
- `get.yml`, `put.yml`, `delete.yml`, `list.yml`, `stat.yml`
- `mixed.yml`, `multipart.yml`, `versioned.yml`, `zip.yml`

---

## Monitoring & Integration

### Web UI

The web UI (`wui/` package) provides real-time visualization during benchmark runs:
- Started with `--web` flag
- Polls for updates from the live aggregation system
- Displays throughput charts and request statistics
- Serves static assets from `wui/static/`

### HTTP API

The `api/` package provides HTTP endpoints for monitoring:
- Benchmark status and progress
- Real-time operation statistics
- Used by both the terminal UI and web UI

### InfluxDB

Real-time metrics can be pushed to InfluxDB v2+:
```
--influxdb https://token@hostname:8086/bucket/org?tag=key=value
```

### Terminal UI

The terminal UI (`cli/ui.go`) uses the [bubbletea](https://github.com/charmbracelet/bubbletea) library:
- Real-time progress bar during prepare phase
- Live throughput and statistics during benchmarks
- Keyboard shortcut ('q') to stop benchmark

---

## Build & Release

### Building

```bash
go build                          # Build binary
go build -ldflags="-s -w"         # Stripped binary
```

### Docker

```bash
# Development build
docker build -t warp:dev -f Dockerfile .

# Release build (minimal)
docker build -t warp:release -f Dockerfile.release .

# Multi-arch release
docker build -t warp:latest -f Dockerfile.v2.release .
```

The Dockerfiles expose port 7761 for distributed client mode.

### Release Process

The project uses GoReleaser (`.goreleaser.yml`) for automated releases:
1. Build for multiple OS/architectures
2. Create Docker images (Dockerfile.v2.release)
3. Upload artifacts to GitHub releases

### CI Pipeline

GitHub Actions workflows (`.github/workflows/`):
- `go.yml` — Build, test with race detection, lint
- `release.yml` — GoReleaser snapshot on push
- `vulncheck.yml` — Vulnerability scanning
- `qreleaser-test.yml` — Q Release pipeline test

---

## Distributed Mode Without Kubernetes

The warp distributed mode uses **plain TCP/WebSocket** — Kubernetes is just a deployment convenience. See `scripts/warp-cluster.sh` for managing warp clients without k8s.

For full details, see the [scripts/README.md](./scripts/README.md) or the warp-cluster script itself.
