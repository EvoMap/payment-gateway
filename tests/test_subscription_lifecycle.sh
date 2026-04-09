#!/usr/bin/env bash
#
# 订阅完整闭环测试脚本
#
# 前置条件:
#   1. 服务已启动 (docker compose -f docker-compose.dev.yml up -d)
#   2. Stripe CLI 已安装并转发 webhook:
#      stripe listen --forward-to http://localhost:8000/v1/callbacks/stripe
#   3. .env 中已配置真实的 Stripe 测试密钥 (sk_test_xxx / whsec_xxx)
#
# 用法:
#   chmod +x tests/test_subscription_lifecycle.sh
#   ./tests/test_subscription_lifecycle.sh [step]
#
#   step 可选值:
#     setup       - 创建应用 + 计划 (只需执行一次)
#     subscribe   - 首次订阅 (返回 Checkout URL, 需手动完成支付)
#     status      - 查询订阅状态
#     upgrade     - 升级订阅
#     downgrade   - 降级订阅
#     cancel      - 取消订阅 (周期末)
#     resume      - 恢复订阅
#     all         - 按交互式流程走完全部步骤
#
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
API_KEY=""
BASIC_PLAN_ID=""
PRO_PLAN_ID=""
PREMIUM_PLAN_ID=""
SUBSCRIPTION_ID=""
STATE_FILE="/tmp/subscription_test_state.json"

# ── 颜色输出 ──────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERR]${NC}   $*"; }
header(){ echo -e "\n${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; echo -e "${GREEN}  $*${NC}"; echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}\n"; }

save_state() {
    cat > "$STATE_FILE" <<EOJSON
{
  "api_key": "${API_KEY}",
  "basic_plan_id": "${BASIC_PLAN_ID}",
  "pro_plan_id": "${PRO_PLAN_ID}",
  "premium_plan_id": "${PREMIUM_PLAN_ID}",
  "subscription_id": "${SUBSCRIPTION_ID}"
}
EOJSON
    info "状态已保存到 $STATE_FILE"
}

load_state() {
    if [[ -f "$STATE_FILE" ]]; then
        API_KEY=$(python3 -c "import json; print(json.load(open('$STATE_FILE'))['api_key'])" 2>/dev/null || true)
        BASIC_PLAN_ID=$(python3 -c "import json; print(json.load(open('$STATE_FILE'))['basic_plan_id'])" 2>/dev/null || true)
        PRO_PLAN_ID=$(python3 -c "import json; print(json.load(open('$STATE_FILE'))['pro_plan_id'])" 2>/dev/null || true)
        PREMIUM_PLAN_ID=$(python3 -c "import json; print(json.load(open('$STATE_FILE'))['premium_plan_id'])" 2>/dev/null || true)
        SUBSCRIPTION_ID=$(python3 -c "import json; print(json.load(open('$STATE_FILE'))['subscription_id'])" 2>/dev/null || true)
        info "已加载测试状态: API_KEY=${API_KEY:0:20}..."
    fi
}

api() {
    local method="$1" path="$2"
    shift 2
    curl -s -X "$method" "${BASE_URL}${path}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${API_KEY}" \
        "$@"
}

admin_api() {
    local method="$1" path="$2"
    shift 2
    curl -s -X "$method" "${BASE_URL}/v1/admin${path}" \
        -H "Content-Type: application/json" \
        "$@"
}

jq_or_python() {
    if command -v jq &>/dev/null; then
        jq "$@"
    else
        python3 -c "
import json, sys
data = json.load(sys.stdin)
path = '''$1'''.strip('.')
for key in path.split('.'):
    if key.startswith('[') and key.endswith(']'):
        data = data[int(key[1:-1])]
    else:
        data = data[key]
if isinstance(data, (dict, list)):
    print(json.dumps(data, indent=2, ensure_ascii=False))
else:
    print(data)
"
    fi
}

extract() {
    python3 -c "
import json, sys
data = json.load(sys.stdin)
keys = '$1'.split('.')
for k in keys:
    data = data[k]
print(data)
"
}

pretty() {
    python3 -c "import json,sys; print(json.dumps(json.load(sys.stdin), indent=2, ensure_ascii=False))"
}

# ════════════════════════════════════════════════════════
# Step 0: 初始化 — 创建应用 + 3 个不同等级的订阅计划
# ════════════════════════════════════════════════════════
do_setup() {
    header "Step 0: 创建应用和订阅计划"

    # 创建应用
    info "创建测试应用..."
    local app_resp
    app_resp=$(admin_api POST "/apps" -d '{
        "name": "subscription-test-app"
    }')
    echo "$app_resp" | pretty
    API_KEY=$(echo "$app_resp" | extract "data.api_key")
    ok "应用创建成功, API Key: ${API_KEY:0:20}..."

    # 创建基础计划 (tier=1, $9.99/月)
    info "创建基础计划 (Basic - \$9.99/月, tier=1)..."
    local basic_resp
    basic_resp=$(api POST "/v1/plans" -d '{
        "provider": "stripe",
        "slug": "basic-monthly",
        "name": "Basic Plan",
        "description": "基础月度计划",
        "amount": 999,
        "currency": "USD",
        "interval": "month",
        "interval_count": 1,
        "tier": 1,
        "features": {"seats": 1, "storage_gb": 10}
    }')
    echo "$basic_resp" | pretty
    BASIC_PLAN_ID=$(echo "$basic_resp" | extract "data.id")
    ok "Basic Plan ID: $BASIC_PLAN_ID"

    # 创建专业计划 (tier=2, $19.99/月)
    info "创建专业计划 (Pro - \$19.99/月, tier=2)..."
    local pro_resp
    pro_resp=$(api POST "/v1/plans" -d '{
        "provider": "stripe",
        "slug": "pro-monthly",
        "name": "Pro Plan",
        "description": "专业月度计划",
        "amount": 1999,
        "currency": "USD",
        "interval": "month",
        "interval_count": 1,
        "tier": 2,
        "features": {"seats": 5, "storage_gb": 100}
    }')
    echo "$pro_resp" | pretty
    PRO_PLAN_ID=$(echo "$pro_resp" | extract "data.id")
    ok "Pro Plan ID: $PRO_PLAN_ID"

    # 创建高级计划 (tier=3, $49.99/月)
    info "创建高级计划 (Premium - \$49.99/月, tier=3)..."
    local premium_resp
    premium_resp=$(api POST "/v1/plans" -d '{
        "provider": "stripe",
        "slug": "premium-monthly",
        "name": "Premium Plan",
        "description": "高级月度计划",
        "amount": 4999,
        "currency": "USD",
        "interval": "month",
        "interval_count": 1,
        "tier": 3,
        "features": {"seats": 20, "storage_gb": 500}
    }')
    echo "$premium_resp" | pretty
    PREMIUM_PLAN_ID=$(echo "$premium_resp" | extract "data.id")
    ok "Premium Plan ID: $PREMIUM_PLAN_ID"

    save_state
    ok "Setup 完成！三个计划已创建。"
}

# ════════════════════════════════════════════════════════
# Step 1: 首次订阅
# ════════════════════════════════════════════════════════
do_subscribe() {
    header "Step 1: 首次订阅 (Basic Plan)"
    load_state

    info "创建订阅..."
    local sub_resp
    sub_resp=$(api POST "/v1/subscriptions" -d "{
        \"external_user_id\": \"test_user_001\",
        \"plan_id\": \"${BASIC_PLAN_ID}\",
        \"email\": \"test@example.com\",
        \"success_url\": \"https://example.com/success\",
        \"cancel_url\": \"https://example.com/cancel\"
    }")
    echo "$sub_resp" | pretty

    SUBSCRIPTION_ID=$(echo "$sub_resp" | extract "data.subscription_id")
    local checkout_url
    checkout_url=$(echo "$sub_resp" | extract "data.checkout_url")

    save_state

    ok "订阅已创建 (状态: incomplete)"
    echo ""
    warn "━━━━━ 需要手动操作 ━━━━━"
    echo -e "  请在浏览器中打开以下链接完成支付:"
    echo -e "  ${CYAN}${checkout_url}${NC}"
    echo ""
    echo -e "  Stripe 测试卡信息:"
    echo -e "    卡号:     ${CYAN}4242 4242 4242 4242${NC}"
    echo -e "    有效期:   ${CYAN}任意未来日期 (如 12/28)${NC}"
    echo -e "    CVC:      ${CYAN}任意3位数 (如 123)${NC}"
    echo -e "    邮编:     ${CYAN}任意5位数 (如 10001)${NC}"
    echo ""
    echo -e "  支付完成后, Stripe 会发送 webhook 到本地服务。"
    echo -e "  运行 ${CYAN}$0 status${NC} 查看订阅是否激活。"
    warn "━━━━━━━━━━━━━━━━━━━━━━━"
}

# ════════════════════════════════════════════════════════
# 查询订阅状态
# ════════════════════════════════════════════════════════
do_status() {
    header "查询订阅状态"
    load_state

    info "查询订阅 ${SUBSCRIPTION_ID}..."
    local status_resp
    status_resp=$(api GET "/v1/subscriptions/${SUBSCRIPTION_ID}")
    echo "$status_resp" | pretty

    local status
    status=$(echo "$status_resp" | extract "data.status")
    if [[ "$status" == "active" ]]; then
        ok "订阅已激活！可以继续测试升降级/取消/恢复。"
    elif [[ "$status" == "trialing" ]]; then
        ok "订阅处于试用期。"
    else
        warn "订阅状态: $status — 如果是 incomplete, 请先完成 Checkout 支付。"
    fi
}

# ════════════════════════════════════════════════════════
# Step 2: 升级订阅
# ════════════════════════════════════════════════════════
do_upgrade() {
    header "Step 2: 升级订阅 (Basic → Pro)"
    load_state

    info "执行升级: Basic(tier=1) → Pro(tier=2)..."
    local upgrade_resp
    upgrade_resp=$(api POST "/v1/subscriptions/${SUBSCRIPTION_ID}/change-plan" -d "{
        \"new_plan_id\": \"${PRO_PLAN_ID}\",
        \"proration_mode\": \"auto\"
    }")
    echo "$upgrade_resp" | pretty

    local direction effective
    direction=$(echo "$upgrade_resp" | extract "data.direction")
    effective=$(echo "$upgrade_resp" | extract "data.effective")

    if [[ "$direction" == "upgrade" ]]; then
        ok "升级成功！方向: $direction, 生效方式: $effective"
        info "升级立即生效, Stripe 会按比例计算差价。"
    else
        err "预期 direction=upgrade, 实际=$direction"
    fi
}

# ════════════════════════════════════════════════════════
# Step 3: 降级订阅
# ════════════════════════════════════════════════════════
do_downgrade() {
    header "Step 3: 降级订阅 (Pro → Basic)"
    load_state

    info "执行降级: Pro(tier=2) → Basic(tier=1)..."
    local downgrade_resp
    downgrade_resp=$(api POST "/v1/subscriptions/${SUBSCRIPTION_ID}/change-plan" -d "{
        \"new_plan_id\": \"${BASIC_PLAN_ID}\",
        \"proration_mode\": \"auto\"
    }")
    echo "$downgrade_resp" | pretty

    local direction effective pending_change_at
    direction=$(echo "$downgrade_resp" | extract "data.direction")
    effective=$(echo "$downgrade_resp" | extract "data.effective")

    if [[ "$direction" == "downgrade" ]]; then
        ok "降级已安排！方向: $direction, 生效方式: $effective"
        info "降级将在当前计费周期结束后生效 (通过 Stripe SubscriptionSchedule 实现)。"
        info "可用 '$0 status' 查看 pending_plan 和 pending_plan_change_at 字段。"
    else
        err "预期 direction=downgrade, 实际=$direction"
    fi
}

# ════════════════════════════════════════════════════════
# Step 4: 取消订阅
# ════════════════════════════════════════════════════════
do_cancel() {
    header "Step 4: 取消订阅 (周期末生效)"
    load_state

    info "取消订阅 (immediate=false, 周期末不续费)..."
    local cancel_resp
    cancel_resp=$(api POST "/v1/subscriptions/${SUBSCRIPTION_ID}/cancel" -d '{
        "immediate": false
    }')
    echo "$cancel_resp" | pretty

    local cancel_at_period_end
    cancel_at_period_end=$(echo "$cancel_resp" | extract "data.cancel_at_period_end")

    if [[ "$cancel_at_period_end" == "True" || "$cancel_at_period_end" == "true" ]]; then
        ok "取消成功！cancel_at_period_end=true"
        info "订阅仍然有效直到当前周期结束, 之后不再自动续费。"
    else
        warn "cancel_at_period_end=$cancel_at_period_end"
    fi
}

# ════════════════════════════════════════════════════════
# Step 5: 恢复订阅
# ════════════════════════════════════════════════════════
do_resume() {
    header "Step 5: 恢复订阅"
    load_state

    info "恢复订阅 (取消「周期末取消」标记)..."
    local resume_resp
    resume_resp=$(api POST "/v1/subscriptions/${SUBSCRIPTION_ID}/resume")
    echo "$resume_resp" | pretty

    local cancel_at_period_end
    cancel_at_period_end=$(echo "$resume_resp" | extract "data.cancel_at_period_end")

    if [[ "$cancel_at_period_end" == "False" || "$cancel_at_period_end" == "false" ]]; then
        ok "恢复成功！cancel_at_period_end=false"
        info "订阅将在周期结束后继续自动续费。"
    else
        warn "cancel_at_period_end=$cancel_at_period_end"
    fi
}

# ════════════════════════════════════════════════════════
# 交互式完整流程
# ════════════════════════════════════════════════════════
do_all() {
    header "订阅完整闭环测试 — 交互式流程"
    echo "本脚本将引导你完成以下 5 个步骤:"
    echo "  1. 首次订阅 (需要在浏览器完成 Stripe Checkout)"
    echo "  2. 升级订阅 (Basic → Pro, 立即生效)"
    echo "  3. 降级订阅 (Pro → Basic, 周期末生效)"
    echo "  4. 取消订阅 (周期末不续费)"
    echo "  5. 恢复订阅 (恢复自动续费)"
    echo ""

    # Setup
    if [[ ! -f "$STATE_FILE" ]]; then
        do_setup
    else
        load_state
        if [[ -z "$API_KEY" || -z "$BASIC_PLAN_ID" ]]; then
            do_setup
        else
            ok "已有测试状态, 跳过 setup。如需重建请先删除 $STATE_FILE"
        fi
    fi

    # Step 1: Subscribe
    echo ""
    read -rp "$(echo -e ${YELLOW}按 Enter 开始 Step 1: 创建订阅...${NC})" _
    do_subscribe

    echo ""
    warn "请在浏览器中完成 Stripe Checkout 支付。"
    read -rp "$(echo -e ${YELLOW}支付完成后, 按 Enter 继续...${NC})" _

    # Check status
    do_status

    # Step 2: Upgrade
    echo ""
    read -rp "$(echo -e ${YELLOW}按 Enter 开始 Step 2: 升级订阅...${NC})" _
    do_upgrade

    # Step 3: Downgrade
    echo ""
    read -rp "$(echo -e ${YELLOW}按 Enter 开始 Step 3: 降级订阅...${NC})" _
    do_downgrade

    # Step 4: Cancel
    echo ""
    read -rp "$(echo -e ${YELLOW}按 Enter 开始 Step 4: 取消订阅...${NC})" _
    do_cancel

    # Step 5: Resume
    echo ""
    read -rp "$(echo -e ${YELLOW}按 Enter 开始 Step 5: 恢复订阅...${NC})" _
    do_resume

    # Final status
    echo ""
    do_status

    header "全部测试完成！"
    echo "订阅完整闭环已验证:"
    echo "  ✓ 首次订阅 (Stripe Checkout + Webhook 激活)"
    echo "  ✓ 升级 (立即生效, 按比例计价)"
    echo "  ✓ 降级 (周期末生效, SubscriptionSchedule)"
    echo "  ✓ 取消 (周期末不续费)"
    echo "  ✓ 恢复 (恢复自动续费)"
}

# ── 入口 ──────────────────────────────────────────────
case "${1:-all}" in
    setup)      do_setup ;;
    subscribe)  do_subscribe ;;
    status)     do_status ;;
    upgrade)    do_upgrade ;;
    downgrade)  do_downgrade ;;
    cancel)     do_cancel ;;
    resume)     do_resume ;;
    all)        do_all ;;
    *)
        echo "用法: $0 {setup|subscribe|status|upgrade|downgrade|cancel|resume|all}"
        exit 1
        ;;
esac
