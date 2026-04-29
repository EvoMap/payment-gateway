"""
WebhookDelivery 投递 Worker（异步轮询 + 并发投递 + 重试 + 死信 + 订阅清理）
"""

import uuid
import asyncio
import random
from datetime import datetime, timedelta, UTC

import hashlib
import hmac

import httpx
import structlog
from sqlalchemy import select, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from gateway.core.constants import DeliveryStatus, SubscriptionStatus
from .db import init_db, close_db, get_session_ctx
from gateway.core.models import WebhookDelivery, Subscription
from gateway.core.settings import get_settings

logger = structlog.get_logger(__name__)
settings = get_settings()

PROCESSING_TIMEOUT = timedelta(minutes=5)


class WebhookDeliveryWorker:
    """WebhookDelivery 投递 Worker"""

    def __init__(self):
        self.poll_interval = settings.worker_poll_interval
        self.batch_size = settings.worker_batch_size
        self.max_retries = settings.worker_max_retries
        self.concurrency = settings.worker_concurrency
        self.http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=5.0, read=10.0, write=10.0, pool=5.0),
            limits=httpx.Limits(
                max_connections=100,
                max_keepalive_connections=20,
                keepalive_expiry=30,
            ),
        )

    async def start(self):
        """启动 worker：投递循环 + 清理循环并行运行"""
        logger.info(
            "Webhook投递Worker启动",
            poll_interval=self.poll_interval,
            batch_size=self.batch_size,
            max_retries=self.max_retries,
            concurrency=self.concurrency,
        )

        try:
            await asyncio.gather(
                self._delivery_loop(),
                self._cleanup_loop(),
            )
        finally:
            await self.http_client.aclose()

    async def _delivery_loop(self):
        """投递主循环"""
        while True:
            try:
                await self.process_pending_deliveries()
            except Exception as exc:
                logger.error(
                    "Webhook投递Worker运行异常",
                    error=str(exc),
                    exc_info=True,
                )
            await asyncio.sleep(self.poll_interval)

    async def _cleanup_loop(self):
        """清理循环（低频）"""
        while True:
            await asyncio.sleep(settings.subscription_cleanup_interval)
            try:
                await self.cleanup_stale_incomplete_subscriptions()
            except Exception as exc:
                logger.error("清理任务异常", error=str(exc), exc_info=True)

    async def process_pending_deliveries(self):
        delivery_ids: list[uuid.UUID] = []

        async with get_session_ctx() as session:
            now = datetime.now(UTC)
            stmt = (
                select(WebhookDelivery)
                .where(
                    and_(
                        WebhookDelivery.attempt_count < self.max_retries,
                        or_(
                            and_(
                                WebhookDelivery.status.in_(
                                    [DeliveryStatus.pending, DeliveryStatus.failed]
                                ),
                                (
                                    WebhookDelivery.next_attempt_at.is_(None)
                                    | (WebhookDelivery.next_attempt_at <= now)
                                ),
                            ),
                            and_(
                                WebhookDelivery.status == DeliveryStatus.processing,
                                WebhookDelivery.last_attempt_at
                                <= now - PROCESSING_TIMEOUT,
                            ),
                        ),
                    )
                )
                .order_by(WebhookDelivery.created_at)
                .limit(self.batch_size)
                .with_for_update(skip_locked=True)
            )

            result = await session.execute(stmt)
            deliveries = result.scalars().all()

            if not deliveries:
                return

            logger.info("开始处理Webhook投递批次", count=len(deliveries))

            for d in deliveries:
                d.status = DeliveryStatus.processing
                d.last_attempt_at = now
                delivery_ids.append(d.id)

        sem = asyncio.Semaphore(self.concurrency)

        async def _deliver_one(delivery_id: uuid.UUID):
            async with sem:
                try:
                    async with get_session_ctx() as session:
                        stmt = select(WebhookDelivery).where(
                            WebhookDelivery.id == delivery_id
                        )
                        result = await session.execute(stmt)
                        delivery = result.scalar_one_or_none()
                        if delivery:
                            await self.deliver_webhook(session, delivery)
                except Exception as exc:
                    logger.error(
                        "Webhook投递任务异常",
                        delivery_id=str(delivery_id),
                        error=str(exc),
                        exc_info=True,
                    )
                    try:
                        async with get_session_ctx() as session:
                            stmt = select(WebhookDelivery).where(
                                WebhookDelivery.id == delivery_id
                            )
                            result = await session.execute(stmt)
                            delivery = result.scalar_one_or_none()
                            if (
                                delivery
                                and delivery.status != DeliveryStatus.succeeded
                            ):
                                delivery.last_error = (
                                    f"WorkerError: {str(exc)[:200]}"
                                )
                                await self.schedule_retry(
                                    session,
                                    delivery,
                                    logger.bind(
                                        delivery_id=str(delivery_id)
                                    ),
                                )
                    except Exception as retry_exc:
                        logger.error(
                            "重试调度失败，等待超时恢复",
                            delivery_id=str(delivery_id),
                            error=str(retry_exc),
                        )

        await asyncio.gather(
            *[_deliver_one(d_id) for d_id in delivery_ids],
            return_exceptions=True,
        )

    async def deliver_webhook(
        self, session: AsyncSession, delivery: WebhookDelivery
    ):
        if delivery.status != DeliveryStatus.processing:
            return

        delivery.attempt_count += 1
        log = logger.bind(
            delivery_id=str(delivery.id),
            event_id=delivery.event_id,
            event_type=delivery.event_type,
            attempt_count=delivery.attempt_count,
        )

        try:
            import json as _json
            body_bytes = _json.dumps(delivery.payload, ensure_ascii=False).encode("utf-8")
            headers = {"Content-Type": "application/json"}
            if settings.webhook_signing_secret:
                sig = hmac.new(
                    settings.webhook_signing_secret.encode("utf-8"),
                    body_bytes,
                    hashlib.sha256,
                ).hexdigest()
                headers["X-Webhook-Signature"] = sig
            response = await self.http_client.post(
                delivery.notify_url,
                content=body_bytes,
                headers=headers,
            )

            delivery.last_http_status = response.status_code

            if 200 <= response.status_code < 300:
                try:
                    await session.refresh(delivery, with_for_update=True)
                except Exception:
                    return

                if delivery.status != DeliveryStatus.processing:
                    return

                delivery.status = DeliveryStatus.succeeded
                delivery.delivered_at = datetime.now(UTC)
                delivery.next_attempt_at = None
                await session.commit()
                log.info("Webhook投递成功", http_status=response.status_code)
            else:
                delivery.last_error = (
                    f"HTTP {response.status_code}: {response.text[:200]}"
                )
                await self.schedule_retry(session, delivery, log)

        except httpx.RequestError as exc:
            delivery.last_http_status = None
            delivery.last_error = f"RequestError: {str(exc)[:200]}"
            await self.schedule_retry(session, delivery, log)

        except Exception as exc:
            delivery.last_http_status = None
            delivery.last_error = f"Exception: {str(exc)[:200]}"
            await self.schedule_retry(session, delivery, log)

    async def schedule_retry(
        self,
        session: AsyncSession,
        delivery: WebhookDelivery,
        log: structlog.BoundLogger,
    ):
        if delivery.attempt_count >= self.max_retries:
            delivery.status = DeliveryStatus.dead
            delivery.next_attempt_at = None
            await session.commit()
            log.error("Webhook投递进入死信", last_error=delivery.last_error)
        else:
            base_delay = 2**delivery.attempt_count
            jitter = random.uniform(0, base_delay * 0.2)
            delay_seconds = base_delay + jitter

            delivery.status = DeliveryStatus.failed
            delivery.next_attempt_at = datetime.now(UTC) + timedelta(
                seconds=delay_seconds
            )
            await session.commit()
            log.warning(
                "Webhook投递失败将重试",
                last_error=delivery.last_error,
                next_attempt_at=delivery.next_attempt_at.isoformat(),
            )

    async def cleanup_stale_incomplete_subscriptions(self):
        """定时清理超期 incomplete 订阅"""
        threshold = datetime.now(UTC) - timedelta(
            minutes=settings.subscription_incomplete_cleanup_minutes
        )

        async with get_session_ctx() as session:
            stmt = (
                select(Subscription)
                .where(
                    Subscription.status
                    == SubscriptionStatus.incomplete.value,
                    Subscription.created_at < threshold,
                )
                .with_for_update(skip_locked=True)
                .limit(100)
            )
            result = await session.execute(stmt)
            stale_subs = result.scalars().all()

            if stale_subs:
                for sub in stale_subs:
                    sub.status = SubscriptionStatus.incomplete_expired.value
                await session.flush()

                from gateway.services.callbacks import CallbackService

                svc = CallbackService(session)
                for sub in stale_subs:
                    await svc.notify_subscription_event(
                        sub, "incomplete_expired", f"cleanup_{sub.id}"
                    )

                logger.info(
                    "清理超期 incomplete 订阅", count=len(stale_subs)
                )


async def main():
    """Worker 入口"""
    await init_db()
    try:
        worker = WebhookDeliveryWorker()
        await worker.start()
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
