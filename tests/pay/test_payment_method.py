"""
测试 payment_method / payment_options 验证逻辑
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from gateway.core.constants import PaymentMethod, Currency
from gateway.core.schemas import CreatePaymentRequest
from gateway.providers.stripe import StripeAdapter


class TestCreatePaymentRequestValidation:
    """schema 层验证"""

    def _base_payload(self, **overrides) -> dict:
        data = {
            "merchant_order_no": "TEST_001",
            "provider": "stripe",
            "currency": "USD",
            "quantity": 1,
            "unit_amount": 1000,
        }
        data.update(overrides)
        return data

    def test_payment_options_without_method_raises(self):
        with pytest.raises(ValueError, match="必须同时指定 payment_method"):
            CreatePaymentRequest(**self._base_payload(
                payment_options={"client": "web"},
            ))

    def test_wechat_pay_valid_clients(self):
        for client in ("web", "ios", "android"):
            req = CreatePaymentRequest(**self._base_payload(
                currency="CNY",
                payment_method="wechat_pay",
                payment_options={"client": client},
            ))
            assert req.payment_method == PaymentMethod.wechat_pay

    def test_wechat_pay_no_options_is_valid(self):
        req = CreatePaymentRequest(**self._base_payload(
            currency="CNY",
            payment_method="wechat_pay",
        ))
        assert req.payment_method == PaymentMethod.wechat_pay

    def test_wechat_pay_invalid_client_raises(self):
        with pytest.raises(ValueError, match="web / ios / android"):
            CreatePaymentRequest(**self._base_payload(
                currency="CNY",
                payment_method="wechat_pay",
                payment_options={"client": "desktop"},
            ))

    def test_wechat_pay_extra_keys_raises(self):
        with pytest.raises(ValueError, match="多余字段"):
            CreatePaymentRequest(**self._base_payload(
                currency="CNY",
                payment_method="wechat_pay",
                payment_options={"client": "web", "foo": "bar"},
            ))

    def test_wechat_pay_unsupported_currency_raises(self):
        # USD is not in WECHAT_PAY_SUPPORTED_CURRENCIES (Stripe limits wechat_pay to CNY/HKD)
        with pytest.raises(ValueError, match="wechat_pay 仅支持"):
            CreatePaymentRequest(**self._base_payload(
                currency="USD",
                payment_method="wechat_pay",
                payment_options={"client": "web"},
            ))

    def test_wechat_pay_supported_currencies(self):
        for cur in ("CNY", "HKD"):
            req = CreatePaymentRequest(**self._base_payload(
                currency=cur,
                payment_method="wechat_pay",
                payment_options={"client": "web"},
            ))
            assert req.payment_method == PaymentMethod.wechat_pay
            assert req.currency.value == cur

    def test_alipay_rejects_payment_options(self):
        with pytest.raises(ValueError, match="alipay 不需要 payment_options"):
            CreatePaymentRequest(**self._base_payload(
                payment_method="alipay",
                payment_options={"client": "web"},
            ))

    def test_alipay_unsupported_currency_raises(self):
        with pytest.raises(ValueError, match="alipay 仅支持"):
            CreatePaymentRequest(**self._base_payload(
                payment_method="alipay",
                currency="JPY",
            ))

    def test_alipay_supported_currencies(self):
        for cur in ("CNY", "USD", "HKD", "EUR", "GBP"):
            req = CreatePaymentRequest(**self._base_payload(
                payment_method="alipay",
                currency=cur,
            ))
            assert req.payment_method == PaymentMethod.alipay

    def test_card_rejects_payment_options(self):
        with pytest.raises(ValueError, match="card 不需要 payment_options"):
            CreatePaymentRequest(**self._base_payload(
                payment_method="card",
                payment_options={"something": "value"},
            ))

    def test_card_without_options_is_valid(self):
        req = CreatePaymentRequest(**self._base_payload(
            payment_method="card",
        ))
        assert req.payment_method == PaymentMethod.card

    def test_no_payment_method_no_options_is_valid(self):
        req = CreatePaymentRequest(**self._base_payload())
        assert req.payment_method is None
        assert req.payment_options is None


class TestStripeSessionBuilder:
    """Stripe adapter session 构建逻辑"""

    @pytest.fixture
    def adapter(self):
        adapter = StripeAdapter()
        adapter._initialized = True
        return adapter

    @pytest.mark.asyncio
    async def test_wechat_pay_sets_payment_method_options(self, adapter):
        mock_session = MagicMock()
        mock_session.url = "https://checkout.stripe.com/session/123"
        mock_session.id = "cs_test_123"

        with patch("stripe.checkout.Session.create_async", new_callable=AsyncMock, return_value=mock_session) as mock_create:
            result = await adapter.create_payment(
                currency="CNY",
                merchant_order_no="TEST_WX_001",
                quantity=1,
                unit_amount=1000,
                notify_url="https://example.com/notify",
                payment_method=PaymentMethod.wechat_pay,
                payment_options={"client": "web"},
            )

            call_kwargs = mock_create.call_args[1]
            assert call_kwargs["payment_method_types"] == ["wechat_pay"]
            assert call_kwargs["payment_method_options"] == {"wechat_pay": {"client": "web"}}
            assert "payment_intent_data" not in call_kwargs

    @pytest.mark.asyncio
    async def test_wechat_pay_defaults_client_to_web(self, adapter):
        mock_session = MagicMock()
        mock_session.url = "https://checkout.stripe.com/session/123"
        mock_session.id = "cs_test_123"

        with patch("stripe.checkout.Session.create_async", new_callable=AsyncMock, return_value=mock_session) as mock_create:
            await adapter.create_payment(
                currency="CNY",
                merchant_order_no="TEST_WX_002",
                quantity=1,
                unit_amount=1000,
                notify_url="https://example.com/notify",
                payment_method=PaymentMethod.wechat_pay,
            )

            call_kwargs = mock_create.call_args[1]
            assert call_kwargs["payment_method_options"] == {"wechat_pay": {"client": "web"}}

    @pytest.mark.asyncio
    async def test_card_includes_payment_intent_data(self, adapter):
        mock_session = MagicMock()
        mock_session.url = "https://checkout.stripe.com/session/123"
        mock_session.id = "cs_test_123"

        with patch("stripe.checkout.Session.create_async", new_callable=AsyncMock, return_value=mock_session) as mock_create:
            await adapter.create_payment(
                currency="USD",
                merchant_order_no="TEST_CARD_001",
                quantity=1,
                unit_amount=2000,
                notify_url="https://example.com/notify",
                payment_method=PaymentMethod.card,
            )

            call_kwargs = mock_create.call_args[1]
            assert "payment_intent_data" in call_kwargs
            assert call_kwargs["payment_method_types"] == ["card"]
            assert "payment_method_options" not in call_kwargs

    @pytest.mark.asyncio
    async def test_no_payment_method_defaults_to_card(self, adapter):
        mock_session = MagicMock()
        mock_session.url = "https://checkout.stripe.com/session/123"
        mock_session.id = "cs_test_123"

        with patch("stripe.checkout.Session.create_async", new_callable=AsyncMock, return_value=mock_session) as mock_create:
            await adapter.create_payment(
                currency="USD",
                merchant_order_no="TEST_DEFAULT_001",
                quantity=1,
                unit_amount=500,
                notify_url="https://example.com/notify",
            )

            call_kwargs = mock_create.call_args[1]
            assert call_kwargs["payment_method_types"] == ["card"]
            assert "payment_intent_data" in call_kwargs

    @pytest.mark.asyncio
    async def test_alipay_sets_payment_method_types(self, adapter):
        mock_session = MagicMock()
        mock_session.url = "https://checkout.stripe.com/session/456"
        mock_session.id = "cs_test_456"

        with patch("stripe.checkout.Session.create_async", new_callable=AsyncMock, return_value=mock_session) as mock_create:
            result = await adapter.create_payment(
                currency="USD",
                merchant_order_no="TEST_ALIPAY_001",
                quantity=1,
                unit_amount=1000,
                notify_url="https://example.com/notify",
                payment_method=PaymentMethod.alipay,
            )

            call_kwargs = mock_create.call_args[1]
            assert call_kwargs["payment_method_types"] == ["alipay"]
            assert "payment_intent_data" in call_kwargs
            assert "payment_method_options" not in call_kwargs

    @pytest.mark.asyncio
    async def test_fallback_to_card_on_invalid_request_without_explicit_method(self, adapter):
        """未显式指定 payment_method 时，InvalidRequestError 触发 fallback 到 card"""
        import stripe

        mock_session = MagicMock()
        mock_session.url = "https://checkout.stripe.com/session/789"
        mock_session.id = "cs_test_789"

        error = stripe.error.InvalidRequestError(
            message="The payment_method_type is not supported",
            param="payment_method_types",
        )

        with patch(
            "stripe.checkout.Session.create_async",
            new_callable=AsyncMock,
            side_effect=[error, mock_session],
        ) as mock_create:
            result = await adapter.create_payment(
                currency="USD",
                merchant_order_no="TEST_FALLBACK_001",
                quantity=1,
                unit_amount=1000,
                notify_url="https://example.com/notify",
                payment_method_types=["ideal"],
            )

            assert mock_create.call_count == 2
            retry_kwargs = mock_create.call_args_list[1][1]
            assert retry_kwargs["payment_method_types"] == ["card"]
            assert "payment_intent_data" in retry_kwargs

    @pytest.mark.asyncio
    async def test_no_fallback_when_explicit_payment_method(self, adapter):
        """显式指定 payment_method 时，InvalidRequestError 不做 fallback，直接抛出"""
        import stripe

        error = stripe.error.InvalidRequestError(
            message="The payment_method_type is not supported",
            param="payment_method_types",
        )

        with patch(
            "stripe.checkout.Session.create_async",
            new_callable=AsyncMock,
            side_effect=error,
        ):
            with pytest.raises(stripe.error.InvalidRequestError):
                await adapter.create_payment(
                    currency="CNY",
                    merchant_order_no="TEST_NO_FALLBACK_001",
                    quantity=1,
                    unit_amount=1000,
                    notify_url="https://example.com/notify",
                    payment_method=PaymentMethod.wechat_pay,
                    payment_options={"client": "web"},
                )
