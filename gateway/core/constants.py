import enum


class Provider(str, enum.Enum):
    stripe = "stripe"


class Currency(str, enum.Enum):
    USD = "USD"  # 美元
    CNY = "CNY"  # 人民币
    HKD = "HKD"  # 港币
    KRW = "KRW"  # 韩元
    THB = "THB"  # 泰铢
    EUR = "EUR"  # 欧元
    GBP = "GBP"  # 英镑
    JPY = "JPY"  # 日元
    INR = "INR"  # 印度卢比


class PayType(str, enum.Enum):
    """统一下单返回类型"""

    redirect = "redirect"
    form = "form"
    qr = "qr"
    client_secret = "client_secret"


class PaymentStatus(str, enum.Enum):
    pending = "pending"
    succeeded = "succeeded"
    failed = "failed"
    canceled = "canceled"


class CallbackStatus(str, enum.Enum):
    received = "received"
    processing = "processing"
    processed = "processed"
    failed = "failed"


class DeliveryStatus(str, enum.Enum):
    pending = "pending"
    processing = "processing"
    succeeded = "succeeded"
    failed = "failed"
    dead = "dead"


class RefundStatus(str, enum.Enum):
    """退款状态"""

    pending = "pending"  # 退款中
    succeeded = "succeeded"  # 退款成功
    failed = "failed"  # 退款失败
    canceled = "canceled"  # 已取消


class BillingInterval(str, enum.Enum):
    """计费周期（应用层枚举，数据库层使用 String(32) + CHECK 约束）"""

    week = "week"
    month = "month"
    quarter = "quarter"  # Stripe 用 month + interval_count=3 实现
    year = "year"


class SubscriptionStatus(str, enum.Enum):
    """订阅状态（应用层枚举，数据库层使用 String(32) + CHECK 约束）"""

    incomplete = "incomplete"  # 首次支付未完成
    incomplete_expired = "incomplete_expired"  # 首次支付超时
    active = "active"  # 正常活跃
    past_due = "past_due"  # 续费失败（宽限期）
    canceled = "canceled"  # 已取消
    unpaid = "unpaid"  # 未支付
    paused = "paused"  # 已暂停
    trialing = "trialing"  # 试用期中


class ProrationMode(str, enum.Enum):
    auto = "auto"  # Stripe 内置按天按比例计算
    custom = "custom"  # 调用方自定义抵扣金额


class EventCategory(str, enum.Enum):
    """回调事件分类 —— 用于 CallbackService 路由"""

    payment = "payment"
    refund = "refund"
    subscription = "subscription"
    invoice = "invoice"
