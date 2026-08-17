from datetime import date

import pytest
import respx
from httpx import Response

from fmp_weknora_bridge.cache import Cache, FixedWindowLimiter, RateLimitExceeded
from fmp_weknora_bridge.fmp import FMPClient


@pytest.fixture
async def client():
    instance = FMPClient(
        api_key="test-key",
        base_url="https://fmp.example/stable",
        cache=Cache(),
        requests_per_minute=300,
        daily_request_budget=1000,
    )
    yield instance
    await instance.close()


@respx.mock
async def test_quote_uses_header_and_returns_envelope_data(client):
    route = respx.get("https://fmp.example/stable/quote").mock(
        return_value=Response(200, json=[{"symbol": "AAPL", "price": 200}])
    )

    result = await client.quotes(["aapl"], "stock")

    assert result == [{"symbol": "AAPL", "price": 200}]
    assert route.called
    assert route.calls[0].request.headers["apikey"] == "test-key"


async def test_rejects_unsupported_quarterly_statements(client):
    with pytest.raises(ValueError, match="annual fundamentals"):
        await client.statements("AAPL", "income", "quarter", 4)


async def test_rejects_excessive_history_range(client):
    with pytest.raises(ValueError, match="no more than"):
        await client.price_history("AAPL", "1d", date(2010, 1, 1), date(2025, 1, 1))


@respx.mock
async def test_nasdaq_stocks_uses_paginated_company_screener(client):
    route = respx.get("https://fmp.example/stable/company-screener").mock(
        side_effect=[
            Response(
                200,
                json=[
                    {"symbol": "AAPL", "exchangeShortName": "NASDAQ", "isEtf": False},
                    {"symbol": "QQQ", "exchangeShortName": "NASDAQ", "isEtf": True},
                    {"symbol": "IBM", "exchangeShortName": "NYSE", "isEtf": False},
                ],
            )
        ]
    )

    result = await client.nasdaq_stocks()

    assert result == [{"symbol": "AAPL", "exchangeShortName": "NASDAQ", "isEtf": False}]
    assert route.calls[0].request.url.params["exchange"] == "NASDAQ"
    assert route.calls[0].request.url.params["page"] == "0"


def test_rate_limiter_does_not_consume_daily_budget_for_rejected_minute_slot():
    cache = Cache()
    limiter = FixedWindowLimiter(cache, max_per_minute=1, max_per_day=10)

    limiter.acquire()
    with pytest.raises(RateLimitExceeded, match="request-per-minute"):
        limiter.acquire()

    assert cache.get("fmp:daily:" + __import__("time").strftime("%Y%m%d", __import__("time").gmtime())) == 1
