"""
渠道回调 API 路由（重构：适配 event_category 路由）
"""

import structlog
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import APIRouter, Request, Response, Depends

from gateway.db import get_session
from gateway.providers.stripe import get_stripe_adapter
from gateway.services.callbacks import CallbackService
from gateway.core.exceptions import IgnoredException

logger = structlog.get_logger(__name__)

router = APIRouter()


@router.post("/stripe")
async def stripe_callback(
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    body = await request.body()
    headers = dict(request.headers)
    log = logger.bind(provider="stripe")

    try:
        adapter = get_stripe_adapter()
        event = await adapter.parse_and_verify_callback(headers, body)

        svc = CallbackService(session)
        await svc.process_callback(event)

        return Response(status_code=200)
    except IgnoredException as err:
        logger.warning(str(err))
        return Response(status_code=200)
    except Exception as exc:
        log.error("回调处理失败", error=str(exc), exc_info=True)
        return Response(status_code=500)
