from __future__ import annotations

import asyncio
import functools
import hmac
import logging
from contextlib import asynccontextmanager
from datetime import date, datetime
from typing import Any

from fastmcp import FastMCP
from fastmcp.server.auth import AccessToken, TokenVerifier
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Mount, Route

from .cache import Cache
from .fmp import FMPClient
from .observability import MCP_CALLS
from .research import safe_structured_result
from .settings import Settings
from .storage import Repository
from .sync import SyncService
from .weknora import WeKnoraClient

logger = logging.getLogger(__name__)


class ConfiguredTokenVerifier(TokenVerifier):
    """Constant-time verifier for the one bearer secret provided to WeKnora."""

    def __init__(self, token: str):
        super().__init__()
        self._token = token

    async def verify_token(self, token: str) -> AccessToken | None:
        if self._token and hmac.compare_digest(token, self._token):
            return AccessToken(token=token, client_id="weknora", scopes=["fmp:read"])
        return None


class Bridge:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.repository = Repository(settings.database_url)
        self.cache = Cache(settings.redis_url)
        self.fmp = FMPClient(
            api_key=settings.fmp_api_key.get_secret_value(),
            base_url=settings.fmp_base_url,
            cache=self.cache,
            requests_per_minute=settings.fmp_requests_per_minute,
            daily_request_budget=settings.fmp_daily_request_budget,
        )
        self.weknora = WeKnoraClient(
            settings.weknora_base_url,
            settings.weknora_api_key.get_secret_value(),
            settings.weknora_knowledge_base_id,
        )
        self.sync = SyncService(
            self.fmp,
            self.repository,
            self.weknora,
            concurrency=settings.fmp_concurrency,
            bootstrap_limit=settings.sync_bootstrap_limit,
            shard_size=settings.sync_shard_size,
            sync_symbols=settings.sync_symbol_list,
            sync_universes=settings.sync_universe_list,
            rotation_batch_size=settings.sync_rotation_batch_size,
        )
        self._scheduler_task: asyncio.Task[None] | None = None
        self._executed_slots: set[str] = set()

    async def start(self) -> None:
        self.repository.create_tables()
        if self.settings.sync_enabled:
            self._scheduler_task = asyncio.create_task(self._scheduler(), name="fmp-sync-scheduler")

    async def close(self) -> None:
        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass
        await self.fmp.close()
        await self.weknora.close()

    async def _scheduler(self) -> None:
        while True:
            now = datetime.now()
            hourly_slot = now.strftime("hourly:%Y%m%d%H")
            catalog_slot = now.strftime("catalog:%Y%m%d")
            if (
                now.minute == self.settings.sync_hourly_minute
                and hourly_slot not in self._executed_slots
            ):
                self._executed_slots.add(hourly_slot)
                asyncio.create_task(self._run_logged("hourly"))
            if (
                now.hour == self.settings.sync_catalog_hour
                and now.minute == self.settings.sync_hourly_minute
                and catalog_slot not in self._executed_slots
            ):
                self._executed_slots.add(catalog_slot)
                asyncio.create_task(self._run_logged("catalog"))
            self._executed_slots = {
                slot for slot in self._executed_slots if now.strftime("%Y%m%d") in slot
            }
            await asyncio.sleep(20)

    async def _run_logged(self, name: str) -> None:
        try:
            if name == "catalog":
                await self.sync.refresh_catalog()
            else:
                await self.sync.run_hourly_snapshot()
        except Exception:
            logger.exception("scheduled %s synchronization failed", name)

    def run_status(self) -> list[dict[str, Any]]:
        return [
            {
                "id": run.id,
                "name": run.name,
                "status": run.status,
                "started_at": run.started_at.isoformat(),
                "finished_at": run.finished_at.isoformat() if run.finished_at else None,
                "processed": run.processed,
                "written": run.written,
                "error": run.error,
            }
            for run in self.repository.latest_runs()
        ]


def create_mcp(bridge: Bridge) -> FastMCP:
    token = bridge.settings.mcp_bearer_token.get_secret_value()
    auth = ConfiguredTokenVerifier(token) if token else None
    mcp = FastMCP(
        "FMP Financial Data",
        instructions=(
            "Use these tools for current financial data from Financial Modeling Prep. "
            "Always cite the supplied retrieved_at timestamp and provider in responses."
        ),
        auth=auth,
    )

    def tracked(name: str):
        def decorator(function):
            @functools.wraps(function)
            async def wrapped(*args, **kwargs):
                try:
                    result = await function(*args, **kwargs)
                    MCP_CALLS.labels(tool=name, status="ok").inc()
                    return result
                except Exception:
                    MCP_CALLS.labels(tool=name, status="error").inc()
                    raise

            return wrapped

        return decorator

    @mcp.tool()
    @tracked("search_instruments")
    async def search_instruments(
        query: str, asset_type: str = "stock", exchange: str = ""
    ) -> dict[str, Any]:
        """Search stock, ETF, cryptocurrency, or forex instruments by ticker/name."""
        return safe_structured_result(
            await bridge.fmp.search_instruments(query, asset_type, exchange),
            "/stable/search-symbol",
        )

    @mcp.tool()
    @tracked("get_market_quote")
    async def get_market_quote(symbols: list[str], asset_type: str = "stock") -> dict[str, Any]:
        """Get the latest quote for up to 25 stock, ETF, crypto, or forex symbols."""
        return safe_structured_result(await bridge.fmp.quotes(symbols, asset_type), "/stable/quote")

    @mcp.tool()
    @tracked("get_price_history")
    async def get_price_history(
        symbol: str, interval: str, from_date: date, to_date: date
    ) -> dict[str, Any]:
        """Get EOD or intraday price history. The maximum date range is ten years."""
        return safe_structured_result(
            await bridge.fmp.price_history(symbol, interval, from_date, to_date),
            "/stable/historical-price-*",
        )

    @mcp.tool()
    @tracked("get_company_research")
    async def get_company_research(symbol: str) -> dict[str, Any]:
        """Get a company profile, annual financials, available metrics/ratios, and latest news."""
        return safe_structured_result(
            await bridge.fmp.company_research(symbol), "/stable/profile and statements"
        )

    @mcp.tool()
    @tracked("get_financial_statements")
    async def get_financial_statements(
        symbol: str, statement: str = "income", period: str = "annual", limit: int = 4
    ) -> dict[str, Any]:
        """Get annual income, balance sheet, or cash flow statements (FMP Starter scope)."""
        return safe_structured_result(
            await bridge.fmp.statements(symbol, statement, period, limit),
            f"/stable/{statement}-statement",
        )

    @mcp.tool()
    @tracked("get_market_news")
    async def get_market_news(
        asset_type: str = "stock", symbols: list[str] | None = None, limit: int = 10
    ) -> dict[str, Any]:
        """Get recent market news, optionally restricted to up to 25 symbols."""
        return safe_structured_result(
            await bridge.fmp.market_news(asset_type, symbols, limit), "/stable/news/*"
        )

    @mcp.tool()
    @tracked("get_economic_events")
    async def get_economic_events(from_date: date, to_date: date) -> dict[str, Any]:
        """Get the FMP economic calendar for a bounded date range."""
        return safe_structured_result(
            await bridge.fmp.economic_events(from_date, to_date), "/stable/economic-calendar"
        )

    return mcp


def create_app(settings: Settings) -> Starlette:
    bridge = Bridge(settings)
    mcp = create_mcp(bridge)
    mcp_app = mcp.http_app(path="/mcp", transport="streamable-http")

    def authorized(request: Request) -> bool:
        expected = settings.mcp_bearer_token.get_secret_value()
        supplied = request.headers.get("Authorization", "").removeprefix("Bearer ")
        return bool(expected and hmac.compare_digest(supplied, expected))

    async def health(_: Request) -> JSONResponse:
        return JSONResponse({"status": "ok"})

    async def ready(_: Request) -> JSONResponse:
        if not settings.configured:
            return JSONResponse(
                {"status": "not_ready", "reason": "FMP_API_KEY is not configured"}, status_code=503
            )
        try:
            bridge.repository.engine.connect().close()
        except Exception as exc:
            return JSONResponse({"status": "not_ready", "reason": str(exc)}, status_code=503)
        return JSONResponse({"status": "ready"})

    async def runs(request: Request) -> JSONResponse:
        if not authorized(request):
            return JSONResponse({"detail": "Unauthorized"}, status_code=401)
        return JSONResponse({"runs": bridge.run_status()})

    async def trigger(request: Request) -> JSONResponse:
        if not authorized(request):
            return JSONResponse({"detail": "Unauthorized"}, status_code=401)
        name = request.path_params["name"]
        if name == "catalog":
            result = await bridge.sync.refresh_catalog()
        elif name == "hourly":
            result = await bridge.sync.run_hourly_snapshot()
        elif name == "preflight":
            result = await bridge.sync.preflight()
        else:
            return JSONResponse({"detail": "Not found"}, status_code=404)
        return JSONResponse(result)

    async def metrics(request: Request) -> Response:
        if not authorized(request):
            return JSONResponse({"detail": "Unauthorized"}, status_code=401)
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @asynccontextmanager
    async def lifespan(app: Starlette):
        # The child MCP app owns its Streamable HTTP session manager. Its lifespan
        # must run in the parent application or requests fail after initialization.
        async with mcp_app.lifespan(app):
            await bridge.start()
            try:
                yield
            finally:
                await bridge.close()

    return Starlette(
        routes=[
            Route("/health", health),
            Route("/ready", ready),
            Route("/admin/runs", runs),
            Route("/admin/sync/{name}", trigger, methods=["POST"]),
            Route("/metrics", metrics),
            Mount("/", app=mcp_app),
        ],
        lifespan=lifespan,
    )
