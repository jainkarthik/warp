#!/usr/bin/env bash
#
# warp-cluster.sh — Orchestrate multiple warp clients without Kubernetes.
#
# Warp's distributed mode uses plain WebSocket over TCP. This script
# manages warp client processes (local or remote via SSH) so you can
# run benchmarks with multiple clients without needing k8s at all.
#
# Usage:
#   warp-cluster.sh start [options...]
#   warp-cluster.sh run <benchmark-type> [warp-flags...]
#   warp-cluster.sh stop
#   warp-cluster.sh generate-hosts
#
# Quick start (single machine, 4 local clients):
#   warp-cluster.sh start --clients 4
#   warp-cluster.sh run get --host my-s3:9000 --access-key AK --secret-key SK \
#     --concurrent 64 --obj.size 32MiB --duration 5m
#   warp-cluster.sh stop
#
# Quick start (remote machines):
#   warp-cluster.sh start --hosts "192.168.1.10,192.168.1.11,192.168.1.12"
#   warp-cluster.sh run get --host my-s3:9000 --access-key AK --secret-key SK ...
#   warp-cluster.sh stop
#
# Using with systemd (production-style, no k8s):
#   # On each machine, enable the systemd unit from systemd/warp.service
#   # Then generate the host file and run:
#   warp-cluster.sh generate-hosts > /tmp/warp-clients.txt
#   warp get --warp-client file:/tmp/warp-clients.txt --host my-s3:9000 ...
#

set -euo pipefail

# ─────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────
WARP_BIN="${WARP_BIN:-warp}"
DEFAULT_PORT=7761
CLIENTS_DIR="${CLIENTS_DIR:-/tmp/warp-cluster}"
HOSTS_FILE="${HOSTS_FILE:-/tmp/warp-cluster-hosts.txt}"
PID_FILE="${CLIENTS_DIR}/pids.txt"
LOG_DIR="${CLIENTS_DIR}/logs"
CLIENT_COUNT=2
REMOTE_HOSTS=""
REMOTE_USER="${REMOTE_USER:-root}"
REMOTE_SSH_KEY="${REMOTE_SSH_KEY:-}"
START_TIMEOUT="${START_TIMEOUT:-15}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[1;34m'
NC='\033[0m' # No Color

info()  { echo -e "${GREEN}[info]${NC} $*"; }
warn()  { echo -e "${YELLOW}[warn]${NC} $*"; }
error() { echo -e "${RED}[error]${NC} $*" >&2; }
step()  { echo -e "${BLUE}==>${NC} $*"; }

cleanup() {
    local exit_code=$?
    if [ "$exit_code" -ne 0 ] && [ -z "${NO_CLEANUP_ON_ERROR:-}" ]; then
        warn "Error encountered. Cleaning up..."
        do_stop
    fi
    exit "$exit_code"
}

trap cleanup EXIT SIGINT SIGTERM

# ─────────────────────────────────────────────────────────────────────
# Help
# ─────────────────────────────────────────────────────────────────────
usage() {
    cat <<'USAGE'
Usage:
  warp-cluster.sh <command> [options]

Commands:
  start                     Start warp client processes
    --clients N             Number of local clients (default: 2)
    --hosts LIST            Comma-separated remote hosts (e.g., "host1,host2")
    --port PORT             Base port for local clients (default: 7761)
    --user USER             SSH user for remote hosts (default: root)
    --ssh-key PATH          SSH key for remote hosts
    --warp-bin PATH         Path to warp binary (default: "warp" from PATH)

  run <benchmark> [flags]   Run a benchmark against running clients
    Passes all remaining arguments to warp as --warp-client=<hosts>
    and appends --noclear automatically.

  stop                      Stop all warp client processes
    Stops local processes and remote processes started by this tool.

  generate-hosts            Print a host list file for --warp-client file:
    Used with the raw warp command: warp get --warp-client file:hosts.txt ...

Examples:
  # 4 local clients, then benchmark
  warp-cluster.sh start --clients 4
  warp-cluster.sh run get --host minio:9000 --access-key AK --secret-key SK \
    --concurrent 64 --obj.size 32MiB
  warp-cluster.sh stop

  # Remote hosts + local clients combined
  warp-cluster.sh start --clients 2 --hosts "192.168.1.10,192.168.1.11"
  warp-cluster.sh run mixed --host minio:9000 --access-key AK --secret-key SK \
    --duration 10m
  warp-cluster.sh stop

  # Just generate host file for already-running clients (e.g., via systemd)
  warp-cluster.sh generate-hosts > /tmp/clients.txt
  warp get --warp-client file:/tmp/clients.txt --host minio:9000 ...
USAGE
    exit 0
}

# ─────────────────────────────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────────────────────────────
ensure_dir() {
    mkdir -p "$CLIENTS_DIR" "$LOG_DIR"
}

get_local_ports() {
    local count="$1"
    local base="${2:-$DEFAULT_PORT}"
    for i in $(seq 0 $((count - 1))); do
        echo "$((base + i))"
    done
}

get_host_list() {
    # Returns the list of all client addresses (local + remote)
    local count="$1"
    local base="${2:-$DEFAULT_PORT}"

    # Local clients
    for port in $(get_local_ports "$count" "$base"); do
        echo "localhost:${port}"
    done

    # Remote clients
    if [ -n "$REMOTE_HOSTS" ]; then
        IFS=',' read -ra hosts <<< "$REMOTE_HOSTS"
        for host in "${hosts[@]}"; do
            host="$(echo "$host" | xargs)"
            if [[ "$host" == *:* ]]; then
                echo "$host"
            else
                echo "${host}:${DEFAULT_PORT}"
            fi
        done
    fi
}

save_pids() {
    local file="$1"
    shift
    for pid in "$@"; do
        echo "$pid" >> "$file"
    done
}

# ─────────────────────────────────────────────────────────────────────
# Commands
# ─────────────────────────────────────────────────────────────────────

# do_start — start warp client processes
do_start() {
    local clients="$CLIENT_COUNT"
    local base_port="$DEFAULT_PORT"

    # Parse start-specific flags
    local args=()
    while [ $# -gt 0 ]; do
        case "$1" in
            --clients) clients="$2"; shift 2 ;;
            --port)    base_port="$2"; shift 2 ;;
            --hosts)   REMOTE_HOSTS="$2"; shift 2 ;;
            --user)    REMOTE_USER="$2"; shift 2 ;;
            --ssh-key) REMOTE_SSH_KEY="$2"; shift 2 ;;
            --warp-bin) WARP_BIN="$2"; shift 2 ;;
            -h|--help) usage ;;
            *) args+=("$1"); shift ;;
        esac
    done

    ensure_dir
    rm -f "$PID_FILE"

    # Validate warp binary
    if ! command -v "$WARP_BIN" &>/dev/null; then
        error "warp binary '$WARP_BIN' not found in PATH"
        exit 1
    fi

    local client_pids=()
    local total_local=$((clients))
    local total_remote=0

    if [ -n "$REMOTE_HOSTS" ]; then
        IFS=',' read -ra hosts <<< "$REMOTE_HOSTS"
        total_remote=${#hosts[@]}
    fi

    echo ""
    step "Starting warp clients ($total_local local, $total_remote remote)..."
    echo ""

    # ── Start local clients ──
    if [ "$clients" -gt 0 ]; then
        info "Starting $clients local warp client(s) on ports $base_port-$((base_port + clients - 1))"
        for port in $(get_local_ports "$clients" "$base_port"); do
            log_file="${LOG_DIR}/client-${port}.log"
            "$WARP_BIN" client "localhost:${port}" > "$log_file" 2>&1 &
            local pid=$!
            client_pids+=("$pid")
            info "  Local client: port=$port pid=$pid log=$log_file"
        done
    fi

    # ── Start remote clients via SSH ──
    if [ -n "$REMOTE_HOSTS" ]; then
        IFS=',' read -ra hosts <<< "$REMOTE_HOSTS"
        for host in "${hosts[@]}"; do
            host="$(echo "$host" | xargs)"
            # Strip port for SSH host
            ssh_host="$host"
            if [[ "$ssh_host" == *:* ]]; then
                ssh_host="${ssh_host%%:*}"
            fi

            info "Starting remote warp client on $ssh_host..."
            local ssh_cmd=("ssh" "-o" "StrictHostKeyChecking=accept-new")
            if [ -n "$REMOTE_SSH_KEY" ]; then
                ssh_cmd+=("-i" "$REMOTE_SSH_KEY")
            fi
            ssh_cmd+=("${REMOTE_USER}@${ssh_host}" "nohup $WARP_BIN client > /tmp/warp-client.log 2>&1 & echo \$!")

            local pid
            pid=$("${ssh_cmd[@]}") || {
                warn "Failed to start warp client on $ssh_host"
                continue
            }
            # Save remote info
            echo "remote:${ssh_host}:${pid}" >> "$PID_FILE"
            info "  Remote client: host=$ssh_host pid=$pid"
        done
    fi

    # Save local PIDs
    if [ ${#client_pids[@]} -gt 0 ]; then
        for pid in "${client_pids[@]}"; do
            echo "local:${pid}" >> "$PID_FILE"
        done
    fi

    # ── Wait for clients to be ready ──
    echo ""
    step "Waiting for clients to be ready (timeout: ${START_TIMEOUT}s)..."
    local ready=0
    local waited=0
    while [ $waited -lt "$START_TIMEOUT" ]; do
        ready=0
        while IFS= read -r line; do
            [ -z "$line" ] && continue
            host_entry=$(echo "$line" | sed 's/^local://;s/^remote://')
            # For local entries, verify by port; for remote, just count
            if echo "$line" | grep -q "^local:"; then
                # Can't easily check readiness, just trust the process
                if kill -0 "$host_entry" 2>/dev/null; then
                    ready=$((ready + 1))
                fi
            else
                ready=$((ready + 1))
            fi
        done < "$PID_FILE"

        total_expected=$((total_local + total_remote))
        if [ "$ready" -ge "$total_expected" ]; then
            # Extra wait for TCP listeners to be ready
            sleep 2
            echo ""
            info "All $total_expected client(s) ready!"
            generate_hosts_file
            print_hosts
            return 0
        fi
        sleep 1
        waited=$((waited + 1))
        echo -n "."
    done

    echo ""
    error "Timeout waiting for clients to be ready (${START_TIMEOUT}s)"
    do_stop
    exit 1
}

# generate_hosts_file — write the hosts file
generate_hosts_file() {
    mkdir -p "$(dirname "$HOSTS_FILE")"
    > "$HOSTS_FILE"

    local count="$CLIENT_COUNT"
    local base_port="$DEFAULT_PORT"

    # If PID_FILE exists, derive from it, otherwise use defaults
    if [ -f "$PID_FILE" ]; then
        # We need to count local vs remote from our saved state
        local local_count=0
        local remote_hosts_list=()

        while IFS= read -r line; do
            [ -z "$line" ] && continue
            if echo "$line" | grep -q "^local:"; then
                local_count=$((local_count + 1))
            elif echo "$line" | grep -q "^remote:"; then
                remote_host=$(echo "$line" | cut -d: -f2)
                remote_hosts_list+=("$remote_host")
            fi
        done < "$PID_FILE"

        if [ "$local_count" -gt 0 ]; then
            base_port=$((DEFAULT_PORT))
            for port in $(get_local_ports "$local_count" "$base_port"); do
                echo "localhost:${port}" >> "$HOSTS_FILE"
            done
        fi

        for host in "${remote_hosts_list[@]}"; do
            if [[ "$host" == *:* ]]; then
                echo "$host" >> "$HOSTS_FILE"
            else
                echo "${host}:${DEFAULT_PORT}" >> "$HOSTS_FILE"
            fi
        done
    else
        # Fallback: just write default localhost entries
        for port in $(get_local_ports "$CLIENT_COUNT" "$DEFAULT_PORT"); do
            echo "localhost:${port}" >> "$HOSTS_FILE"
        done
    fi
}

print_hosts() {
    echo ""
    info "Client addresses:"
    while IFS= read -r line; do
        [ -z "$line" ] && continue
        addr=$(echo "$line" | sed 's/^local://;s/^remote://')
        if echo "$line" | grep -q "^local:"; then
            # It's a PID for local - derive port from saved state
            local port=$DEFAULT_PORT
            echo "    localhost:${port}"
        elif echo "$line" | grep -q "^remote:"; then
            local host=$(echo "$line" | cut -d: -f2)
            echo "    ${host}:${DEFAULT_PORT}"
        fi
    done < "$PID_FILE"

    echo ""
    info "Hosts file: $HOSTS_FILE"
    echo "    $(cat "$HOSTS_FILE" | tr '\n' ',' | sed 's/,$//')"
    echo ""
}

# do_run — run a benchmark against active clients
do_run() {
    if [ $# -lt 1 ]; then
        error "Usage: warp-cluster.sh run <benchmark-type> [warp-flags...]"
        exit 1
    fi

    local bench_type="$1"
    shift

    # Validate benchmark type
    case "$bench_type" in
        get|put|delete|list|stat|mixed|versioned|retention|multipart|multipart-put|zip|snowball|fanout|append|iceberg) ;;
        *)
            error "Unknown benchmark type: $bench_type"
            echo "Valid: get, put, delete, list, stat, mixed, versioned, retention,"
            echo "       multipart, multipart-put, zip, snowball, fanout, append, iceberg"
            exit 1
            ;;
    esac

    # Build the client list
    generate_hosts_file
    local client_list
    client_list=$(paste -sd, "$HOSTS_FILE")

    if [ -z "$client_list" ]; then
        error "No clients available. Start clients first with 'warp-cluster.sh start'"
        exit 1
    fi

    echo ""
    step "Running $bench_type benchmark against $(wc -l < "$HOSTS_FILE") client(s)..."
    info "Client list: $client_list"
    echo ""

    # Build the command
    local cmd=("$WARP_BIN" "$bench_type" "--warp-client" "$client_list" "--noclear" "$@")
    info "Executing: ${cmd[*]}"
    echo ""

    "${cmd[@]}"
    local exit_code=$?

    if [ "$exit_code" -eq 0 ]; then
        info "Benchmark completed successfully."
    else
        warn "Benchmark exited with code $exit_code"
    fi

    return "$exit_code"
}

# do_stop — stop all warp client processes
do_stop() {
    echo ""
    step "Stopping warp clients..."

    if [ ! -f "$PID_FILE" ]; then
        warn "No PID file found at $PID_FILE. Trying to kill warp client processes..."
        # Best-effort: kill any remaining warp clients started by this tool
        pkill -f "warp client localhost:" 2>/dev/null || true
        if [ -n "$REMOTE_HOSTS" ]; then
            IFS=',' read -ra hosts <<< "$REMOTE_HOSTS"
            for host in "${hosts[@]}"; do
                host="$(echo "$host" | xargs)"
                [[ "$host" == *:* ]] && host="${host%%:*}"
                local ssh_cmd=("ssh")
                [ -n "$REMOTE_SSH_KEY" ] && ssh_cmd+=("-i" "$REMOTE_SSH_KEY")
                ssh_cmd+=("${REMOTE_USER}@${host}" "pkill warp 2>/dev/null; kill %1 2>/dev/null; true")
                "${ssh_cmd[@]}" 2>/dev/null || true
            done
        fi
        return 0
    fi

    local stopped=0
    while IFS= read -r line; do
        [ -z "$line" ] && continue

        if echo "$line" | grep -q "^local:"; then
            local pid=$(echo "$line" | cut -d: -f2)
            if kill -0 "$pid" 2>/dev/null; then
                kill "$pid" 2>/dev/null || true
                info "Stopped local client: pid=$pid"
                stopped=$((stopped + 1))
            fi
        elif echo "$line" | grep -q "^remote:"; then
            local host=$(echo "$line" | cut -d: -f2)
            local ssh_cmd=("ssh" "-o" "ConnectTimeout=5")
            [ -n "$REMOTE_SSH_KEY" ] && ssh_cmd+=("-i" "$REMOTE_SSH_KEY")
            ssh_cmd+=("${REMOTE_USER}@${host}" "pkill warp 2>/dev/null; true")
            "${ssh_cmd[@]}" 2>/dev/null || true
            info "Stopped remote client: host=$host"
            stopped=$((stopped + 1))
        fi
    done < "$PID_FILE"

    rm -f "$PID_FILE"
    info "Stopped $stopped client process(es)."
}

# do_generate_hosts — output a host file for use with --warp-client file:
do_generate_hosts() {
    if [ -f "$PID_FILE" ]; then
        generate_hosts_file
        cat "$HOSTS_FILE"
    else
        warn "No active cluster found. Include the --hosts you want or start the cluster first."
        # If REMOTE_HOSTS is set, generate from that
        if [ -n "$REMOTE_HOSTS" ]; then
            IFS=',' read -ra hosts <<< "$REMOTE_HOSTS"
            for host in "${hosts[@]}"; do
                host="$(echo "$host" | xargs)"
                if [[ "$host" == *:* ]]; then
                    echo "$host"
                else
                    echo "${host}:${DEFAULT_PORT}"
                fi
            done
        else
            # Default localhost entries
            for port in $(get_local_ports "$CLIENT_COUNT" "$DEFAULT_PORT"); do
                echo "localhost:${port}"
            done
        fi
    fi
}

# ─────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────
main() {
    if [ $# -eq 0 ]; then
        usage
    fi

    local cmd="$1"
    shift

    case "$cmd" in
        start)
            do_start "$@"
            ;;
        run)
            do_run "$@"
            ;;
        stop)
            do_stop
            ;;
        generate-hosts|hosts)
            do_generate_hosts
            ;;
        -h|--help|help)
            usage
            ;;
        *)
            error "Unknown command: $cmd"
            echo ""
            usage
            ;;
    esac
}

main "$@"
