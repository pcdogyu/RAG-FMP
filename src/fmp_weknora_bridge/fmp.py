from __future__ import annotations

import asyncio
from datetime import date
from typing import Any

import httpx

from .cache import Cache, FixedWindowLimiter
from .observability import FMP_ERRORS, FMP_REQUESTS

ASSET_TYPES = {"stock", "etf", "crypto", "forex"}
MAX_SYMBOLS_PER_REQUEST = 25
MAX_HISTORY_DAYS = 3660
NASDAQ_SCREENER_PAGE_SIZE = 1000
MAX_NASDAQ_SCREENER_PAGES = 20


class FMPError(RuntimeError):
    pass


class FMPClient:
    def __init__(
        self,
        *,
        api_key: str,
        base_url: str,
        cache: Cache,
        requests_per_minute: int,
        daily_request_budget: int,
        timeout_seconds: float = 20,
    ):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.cache = cache
        self.limiter = FixedWindowLimiter(cache, requests_per_minute, daily_request_budget)
        self.client = httpx.AsyncClient(timeout=timeout_seconds)

    async def close(self) -> None:
        await self.client.aclose()

    async def request(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        if not self.api_key:
            raise FMPError("FMP_API_KEY is not configured")
        self.limiter.acquire()
        params = params or {}
        for attempt in range(3):
            try:
                response = await self.client.get(
                    f"{self.base_url}/{endpoint.lstrip('/')}",
                    params=params,
                    headers={"apikey": self.api_key},
                )
                FMP_REQUESTS.labels(endpoint=endpoint, status=str(response.status_code)).inc()
                if response.status_code == 429:
                    if attempt == 2:
                        raise FMPError("FMP returned 429 rate limited")
                    await asyncio.sleep(2**attempt)
                    continue
                response.raise_for_status()
                payload = response.json()
                if isinstance(payload, dict) and payload.get("Error Message"):
                    raise FMPError(str(payload["Error Message"]))
                return payload
            except (httpx.HTTPError, ValueError) as exc:
                if attempt == 2:
                    FMP_ERRORS.labels(endpoint=endpoint, reason=type(exc).__name__).inc()
                    raise FMPError(f"FMP request failed: {exc}") from exc
                await asyncio.sleep(2**attempt)
        raise AssertionError("unreachable")

    async def cached_request(
        self, endpoint: str, params: dict[str, Any] | None = None, *, ttl_seconds: int = 60
    ) -> Any:
        import hashlib
        import json

        key_payload = json.dumps([endpoint, params or {}], sort_keys=True, default=str)
        key = "fmp:" + hashlib.sha256(key_payload.encode()).hexdigest()
        cached = self.cache.get(key)
        if cached is not None:
            return cached
        payload = await self.request(endpoint, params)
        self.cache.set(key, payload, ttl_seconds)
        return payload

    @staticmethod
    def validate_asset_type(asset_type: str) -> str:
        asset_type = asset_type.lower().strip()
        if asset_type not in ASSET_TYPES:
            raise ValueError(f"asset_type must be one of: {', '.join(sorted(ASSET_TYPES))}")
        return asset_type

    @staticmethod
    def validate_symbols(symbols: list[str]) -> list[str]:
        clean = [symbol.upper().strip() for symbol in symbols if symbol and symbol.strip()]
        if not clean or len(clean) > MAX_SYMBOLS_PER_REQUEST:
            raise ValueError(f"symbols must contain 1 to {MAX_SYMBOLS_PER_REQUEST} values")
        if any(
            len(symbol) > 32 or not all(c.isalnum() or c in ".-_=^" for c in symbol)
            for symbol in clean
        ):
            raise ValueError("symbols contain unsupported characters")
        return clean

    async def search_instruments(self, query: str, asset_type: str, exchange: str = "") -> Any:
        self.validate_asset_type(asset_type)
        if not 1 <= len(query.strip()) <= 80:
            raise ValueError("query must contain 1 to 80 characters")
        data = await self.cached_request(
            "search-symbol", {"query": query.strip()}, ttl_seconds=3600
        )
        if exchange:
            data = [
                item
                for item in data
                if str(item.get("exchangeShortName", "")).upper() == exchange.upper()
            ]
        return data[:50]

    async def quotes(self, symbols: list[str], asset_type: str) -> list[dict[str, Any]]:
        self.validate_asset_type(asset_type)
        clean = self.validate_symbols(symbols)
        # FMP Starter does not authorize batch-quote. Preserve the MCP batch interface
        # while issuing the allowed single-symbol endpoint with a strict caller limit.
        payloads = await asyncio.gather(
            *(self.cached_request("quote", {"symbol": symbol}, ttl_seconds=55) for symbol in clean)
        )
        return [
            item
            for payload in payloads
            for item in (payload if isinstance(payload, list) else [payload])
        ]

    async def price_history(
        self, symbol: str, interval: str, from_date: date, to_date: date
    ) -> Any:
        clean = self.validate_symbols([symbol])[0]
        if from_date > to_date or (to_date - from_date).days > MAX_HISTORY_DAYS:
            raise ValueError(
                f"date range must be positive and no more than {MAX_HISTORY_DAYS} days"
            )
        if interval == "1d":
            endpoint = "historical-price-eod/full"
        elif interval in {"1min", "5min", "15min", "30min", "1hour", "4hour"}:
            endpoint = f"historical-chart/{interval}"
        else:
            raise ValueError("interval must be 1d, 1min, 5min, 15min, 30min, 1hour, or 4hour")
        return await self.cached_request(
            endpoint,
            {"symbol": clean, "from": from_date.isoformat(), "to": to_date.isoformat()},
            ttl_seconds=300,
        )

    async def statements(self, symbol: str, statement: str, period: str, limit: int) -> Any:
        clean = self.validate_symbols([symbol])[0]
        endpoint = {
            "income": "income-statement",
            "balance_sheet": "balance-sheet-statement",
            "cash_flow": "cash-flow-statement",
        }.get(statement)
        if endpoint is None:
            raise ValueError("statement must be income, balance_sheet, or cash_flow")
        if period != "annual":
            raise ValueError(
                "this deployment is configured for FMP Starter annual fundamentals only"
            )
        if not 1 <= limit <= 20:
            raise ValueError("limit must be between 1 and 20")
        return await self.cached_request(
            endpoint, {"symbol": clean, "period": period, "limit": limit}, ttl_seconds=3600
        )

    async def company_research(self, symbol: str) -> dict[str, Any]:
        clean = self.validate_symbols([symbol])[0]
        calls = {
            "profile": self.cached_request("profile", {"symbol": clean}, ttl_seconds=3600),
            "income_statement": self.cached_request(
                "income-statement",
                {"symbol": clean, "period": "annual", "limit": 5},
                ttl_seconds=3600,
            ),
            "key_metrics": self.cached_request(
                "key-metrics", {"symbol": clean, "period": "annual", "limit": 1}, ttl_seconds=3600
            ),
            "ratios": self.cached_request(
                "ratios", {"symbol": clean, "period": "annual", "limit": 1}, ttl_seconds=3600
            ),
            "news": self.cached_request(
                "news/stock", {"symbols": clean, "limit": 10}, ttl_seconds=600
            ),
        }
        values = await asyncio.gather(*calls.values())
        return dict(zip(calls, values, strict=True))

    async def market_news(self, asset_type: str, symbols: list[str] | None, limit: int) -> Any:
        self.validate_asset_type(asset_type)
        if not 1 <= limit <= 50:
            raise ValueError("limit must be between 1 and 50")
        params: dict[str, Any] = {"limit": limit}
        if symbols:
            params["symbols"] = ",".join(self.validate_symbols(symbols))
        endpoint = {"crypto": "news/crypto", "forex": "news/forex"}.get(asset_type, "news/stock")
        return await self.cached_request(endpoint, params, ttl_seconds=300)

    async def economic_events(self, from_date: date, to_date: date) -> Any:
        if from_date > to_date or (to_date - from_date).days > 366:
            raise ValueError("date range must be positive and no more than 366 days")
        return await self.cached_request(
            "economic-calendar",
            {"from": from_date.isoformat(), "to": to_date.isoformat()},
            ttl_seconds=3600,
        )

    async def catalog(self, asset_type: str) -> Any:
        self.validate_asset_type(asset_type)
        endpoint = {
            "stock": "stock-list",
            "etf": "etf-list",
            "crypto": "cryptocurrency-list",
            "forex": "forex-list",
        }[asset_type]
        return await self.cached_request(endpoint, ttl_seconds=23 * 3600)

    async def nasdaq_stocks(self) -> list[dict[str, Any]]:
        """Return active NASDAQ-listed equities using the Starter-authorized screener.

        FMP's nasdaq-constituent endpoint is an index dataset and is not part of
        the Starter entitlement. The exchange-filtered company screener provides
        the required NASDAQ listing directory instead.
        """
        rows: list[dict[str, Any]] = []
        for page in range(MAX_NASDAQ_SCREENER_PAGES):
            payload = await self.cached_request(
                "company-screener",
                {
                    "exchange": "NASDAQ",
                    "isActivelyTrading": "true",
                    "limit": NASDAQ_SCREENER_PAGE_SIZE,
                    "page": page,
                },
                ttl_seconds=23 * 3600,
            )
            page_rows = payload if isinstance(payload, list) else []
            for item in page_rows:
                if not isinstance(item, dict):
                    continue
                exchange = str(item.get("exchangeShortName") or item.get("exchange") or "").upper()
                if exchange == "NASDAQ" and item.get("isEtf") is not True:
                    rows.append(item)
            if len(page_rows) < NASDAQ_SCREENER_PAGE_SIZE:
                break
        else:
            raise FMPError(
                "NASDAQ company screener exceeded the configured pagination limit; "
                "increase MAX_NASDAQ_SCREENER_PAGES"
            )
        by_symbol = {str(item.get("symbol") or "").upper(): item for item in rows}
        return [by_symbol[symbol] for symbol in sorted(by_symbol) if symbol]
