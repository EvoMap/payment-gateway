#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

COMPOSE_PROJECT="payment-gateway"
COMPOSE_FILE="docker-compose.yml"

# ---------- 颜色 ----------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }
log_step()  { echo -e "${CYAN}==>${NC} $*"; }

# ---------- 帮助 ----------
usage() {
    cat <<EOF
用法: $(basename "$0") <命令> [选项]

命令:
  start       启动所有服务（自动构建镜像）
  stop        停止所有服务
  restart     重启所有服务（自动重新构建镜像）
  status      查看服务状态
  logs        查看服务日志（可追加服务名: api / worker / db）
  health      检查 API 健康状态

选项:
  -c, --compose FILE  指定 docker-compose 配置文件（默认: docker-compose.yml）
  -t, --tag TAG    镜像标签（默认: latest）
  -d, --detach     后台运行（默认）
  -f, --follow     前台运行并输出日志
  -h, --help       显示帮助

示例:
  $(basename "$0") start                    # 构建并启动所有服务
  $(basename "$0") start -t v1.2.3          # 指定镜像标签
  $(basename "$0") restart                  # 重新构建并重启
  $(basename "$0") logs api -f              # 追踪 API 日志
EOF
}

# ---------- 前置检查 ----------
preflight() {
    local missing=()
    command -v docker &>/dev/null || missing+=("docker")
    if ! docker compose version &>/dev/null 2>&1; then
        missing+=("docker compose")
    fi
    if [[ ${#missing[@]} -gt 0 ]]; then
        log_error "缺少依赖: ${missing[*]}"
        exit 1
    fi
}

ensure_env_file() {
    if [[ ! -f .env ]]; then
        if [[ -f .env.example ]]; then
            log_warn ".env 文件不存在，从 .env.example 复制模板"
            cp .env.example .env
            log_warn "请编辑 .env 填入实际配置后重新执行"
            exit 1
        else
            log_error "未找到 .env 和 .env.example，请先创建配置文件"
            exit 1
        fi
    fi
}

# ---------- 解析参数 ----------
IMAGE_TAG=""
DETACH=true
COMMAND=""
EXTRA_ARGS=()

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            start|stop|restart|status|logs|health)
                COMMAND="$1"; shift ;;
            -c|--compose)
                COMPOSE_FILE="$2"; shift 2 ;;
            -t|--tag)
                IMAGE_TAG="$2"; shift 2 ;;
            -d|--detach)
                DETACH=true; shift ;;
            -f|--follow)
                DETACH=false; shift ;;
            -h|--help)
                usage; exit 0 ;;
            *)
                EXTRA_ARGS+=("$1"); shift ;;
        esac
    done

    if [[ -z "$COMMAND" ]]; then
        usage
        exit 1
    fi

    if [[ ! -f "$COMPOSE_FILE" ]]; then
        log_error "配置文件不存在: $COMPOSE_FILE"
        exit 1
    fi
}

compose() {
    if [[ -n "$IMAGE_TAG" ]]; then
        IMAGE_TAG="$IMAGE_TAG" docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" "$@"
    else
        docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" "$@"
    fi
}

# ---------- 命令实现 ----------
cmd_start() {
    log_step "构建并启动服务..."
    if $DETACH; then
        compose up -d --build --remove-orphans
    else
        compose up --build --remove-orphans
        return
    fi

    log_info "服务已启动"
    echo ""
    cmd_status
    echo ""
    log_step "等待 API 健康检查..."
    wait_healthy "payment-gateway-api" 60
}

cmd_stop() {
    log_step "停止服务..."
    compose down --remove-orphans
    log_info "服务已停止"
}

cmd_restart() {
    log_step "重新构建并重启服务..."
    compose up -d --build --remove-orphans --force-recreate
    log_info "服务已重启"
    echo ""
    cmd_status
    echo ""
    log_step "等待 API 健康检查..."
    wait_healthy "payment-gateway-api" 60
}

cmd_status() {
    log_step "服务状态:"
    compose ps -a
}

cmd_logs() {
    compose logs --tail=100 "${EXTRA_ARGS[@]}"
}

cmd_health() {
    local port="${API_PORT:-8000}"
    local url="http://127.0.0.1:${port}/health"
    log_step "检查 API 健康: ${url}"

    if curl -sf --max-time 3 "$url" >/dev/null 2>&1; then
        log_info "API 健康 ✓"
        curl -s "$url" | python3 -m json.tool 2>/dev/null || true
    else
        log_error "API 不可达或不健康"
        exit 1
    fi
}

wait_healthy() {
    local container="$1"
    local timeout="$2"
    local elapsed=0

    while [[ $elapsed -lt $timeout ]]; do
        local state
        state=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "missing")

        case "$state" in
            healthy)
                log_info "API 健康检查通过 (${elapsed}s)"
                return 0 ;;
            unhealthy)
                log_error "API 健康检查失败"
                docker logs --tail=20 "$container"
                return 1 ;;
        esac

        sleep 3
        elapsed=$((elapsed + 3))
        printf "\r  等待中... %ds / %ds" "$elapsed" "$timeout"
    done

    echo ""
    log_error "健康检查超时 (${timeout}s)"
    docker logs --tail=20 "$container"
    return 1
}

# ---------- 主流程 ----------
main() {
    parse_args "$@"
    preflight

    case "$COMMAND" in
        start)   ensure_env_file; cmd_start ;;
        stop)    cmd_stop ;;
        restart) ensure_env_file; cmd_restart ;;
        status)  cmd_status ;;
        logs)    cmd_logs ;;
        health)  cmd_health ;;
    esac
}

main "$@"
