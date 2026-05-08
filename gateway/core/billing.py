"""
计费周期计算工具
"""

from datetime import datetime

from dateutil.relativedelta import relativedelta

from gateway.core.constants import BillingInterval


def calculate_period_end(
    start: datetime, interval: str, interval_count: int
) -> datetime:
    """根据计费间隔计算下一个 period_end

    Raises ValueError with a clear message if the interval is unknown,
    so Worker error handlers can surface the root cause.
    """
    if interval == BillingInterval.day.value:
        return start + relativedelta(days=interval_count)
    elif interval == BillingInterval.week.value:
        return start + relativedelta(weeks=interval_count)
    elif interval == BillingInterval.month.value:
        return start + relativedelta(months=interval_count)
    elif interval == BillingInterval.quarter.value:
        return start + relativedelta(months=3 * interval_count)
    elif interval == BillingInterval.year.value:
        return start + relativedelta(years=interval_count)
    raise ValueError(
        f"Unsupported billing interval: '{interval}'. "
        f"Supported: {[e.value for e in BillingInterval]}"
    )
