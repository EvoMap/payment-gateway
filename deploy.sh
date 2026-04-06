#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

COMPOSE_PROJECT="payment-gateway"
DEV_COMPOSE_FILE="docker-compose.dev.yml"
PROD_COMPOSE_FILE="docker-compose.yml"

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
  start       启动所有服务
  stop        停止所有服务
  restart     重启所有服务
  status      查看服务状态
  logs        查看服务日志（可追加服务名: api / worker / db）
  pull        拉取最新镜像
  health      检查 API 健康状态

选项:
  -e, --env ENV    运行环境: dev（含本地数据库） | prod（默认）
  -c, --compose FILE  指定 docker-compose 配置文件（优先于 -e）
  -t, --tag TAG    镜像标签（默认: latest）
  -d, --detach     后台运行（默认）
  -f, --follow     前台运行并输出日志
  -h, --help       显示帮助

示例:
  $(basename "$0") start                              # 生产环境启动
  $(basename "$0") start -e dev                       # 开发环境启动（含本地 PG）
  $(basename "$0") start -c docker-compose.prod.yml   # 指定配置文件
  $(basename "$0") start -t v1.2.3                    # 指定镜像标签
  $(basename "$0") logs api -f                        # 追踪 API 日志
  $(basename "$0") restart -e dev                     # 重启开发环境
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
ENV_MODE="prod"
COMPOSE_FILE=""
IMAGE_TAG=""
DETACH=true
COMMAND=""
EXTRA_ARGS=()

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            start|stop|restart|status|logs|pull|health)
                COMMAND="$1"; shift ;;
            -e|--env)
                ENV_MODE="$2"; shift 2 ;;
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
}

resolve_compose_file() {
    if [[ -n "$COMPOSE_FILE" ]]; then
        if [[ ! -f "$COMPOSE_FILE" ]]; then
            log_error "配置文件不存在: $COMPOSE_FILE"
            exit 1
        fi
        echo "$COMPOSE_FILE"
        return
    fi
    case "$ENV_MODE" in
        dev)  echo "$DEV_COMPOSE_FILE" ;;
        prod) echo "$PROD_COMPOSE_FILE" ;;
        *)    log_error "未知环境: $ENV_MODE（可选: dev / prod）"; exit 1 ;;
    esac
}

compose() {
    local file
    file="$(resolve_compose_file)"
    if [[ -n "$IMAGE_TAG" ]]; then
        IMAGE_TAG="$IMAGE_TAG" docker compose -p "$COMPOSE_PROJECT" -f "$file" "$@"
    else
        docker compose -p "$COMPOSE_PROJECT" -f "$file" "$@"
    fi
}

display_label() {
    if [[ -n "$COMPOSE_FILE" ]]; then
        echo "$COMPOSE_FILE"
    else
        echo "$ENV_MODE"
    fi
}

# ---------- 命令实现 ----------
cmd_pull() {
    log_step "拉取最新镜像 ($(display_label))..."
    compose pull
    log_info "镜像拉取完成"
}

needs_build() {
    local file
    file="$(resolve_compose_file)"
    grep -q '^\s*build:' "$file" 2>/dev/null
}

cmd_start() {
    log_step "启动服务 ($(display_label))..."

    local build_flag=""
    if needs_build; then
        log_info "检测到 build 配置，将即时构建镜像"
        build_flag="--build"
    else
        compose pull
    fi

    if $DETACH; then
        compose up -d --remove-orphans $build_flag
    else
        compose up --remove-orphans $build_flag
        return
    fi

    log_info "服务已启动"
    echo ""
    cmd_status
    echo ""

    if [[ -z "$COMPOSE_FILE" && "$ENV_MODE" == "prod" ]] || [[ -n "$COMPOSE_FILE" ]]; then
        log_step "等待 API 健康检查..."
        wait_healthy "payment-gateway-api" 60
    fi
}

cmd_stop() {
    log_step "停止服务 ($(display_label))..."
    compose down --remove-orphans
    log_info "服务已停止"
}

cmd_restart() {
    log_step "重启服务 ($(display_label))..."

    local build_flag=""
    if needs_build; then
        log_info "检测到 build 配置，将即时构建镜像"
        build_flag="--build"
    else
        compose pull
    fi

    compose up -d --remove-orphans --force-recreate $build_flag
    log_info "服务已重启"
    echo ""
    cmd_status

    if [[ -z "$COMPOSE_FILE" && "$ENV_MODE" == "prod" ]] || [[ -n "$COMPOSE_FILE" ]]; then
        echo ""
        log_step "等待 API 健康检查..."
        wait_healthy "payment-gateway-api" 60
    fi
}

cmd_status() {
    log_step "服务状态 ($(display_label)):"
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
        pull)    ensure_env_file; cmd_pull ;;
        start)   ensure_env_file; cmd_start ;;
        stop)    cmd_stop ;;
        restart) ensure_env_file; cmd_restart ;;
        status)  cmd_status ;;
        logs)    cmd_logs ;;
        health)  cmd_health ;;
    esac
}

main "$@"
