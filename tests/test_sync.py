from pathlib import Path

import pytest

from fmp_weknora_bridge.storage import Repository
from fmp_weknora_bridge.sync import SyncService


class FakeFMP:
    class Limiter:
        max_per_day = 1000

    limiter = Limiter()

    async def cached_request(self, endpoint, params, ttl_seconds):
        return []

    async def quotes(self, symbols, asset_type):
        return [{"symbol": symbols[0], "price": 100, "volume": 10}]

    async def company_research(self, symbol):
        return {
            "profile": [{"companyName": symbol}],
            "income_statement": [],
            "key_metrics": [],
            "ratios": [],
            "news": [],
        }

    async def market_news(self, asset_type, symbols, limit):
        return []


class FakeWeKnora:
    async def preflight(self):
        return None

    async def upsert_markdown(self, title, content, knowledge_id=""):
        return knowledge_id or "wk-document-1"


async def test_snapshot_is_idempotent(tmp_path: Path):
    repo = Repository(f"sqlite:///{tmp_path / 'bridge.db'}")
    repo.create_tables()
    repo.upsert_instrument({"symbol": "AAPL", "name": "Apple"}, "stock")
    service = SyncService(
        FakeFMP(), repo, FakeWeKnora(), concurrency=1, bootstrap_limit=0, shard_size=10
    )

    first = await service.run_hourly_snapshot()
    second = await service.run_hourly_snapshot()

    assert first == {"processed": 1, "written": 1}
    assert second == {"processed": 1, "written": 0}


async def test_preflight_uses_bootstrap_universe_for_budget(tmp_path: Path):
    repo = Repository(f"sqlite:///{tmp_path / 'bridge.db'}")
    repo.create_tables()
    for index in range(100):
        repo.upsert_instrument({"symbol": f"S{index:03d}"}, "stock")
    service = SyncService(
        FakeFMP(), repo, FakeWeKnora(), concurrency=1, bootstrap_limit=1, shard_size=10
    )

    result = await service.preflight()

    assert result["catalog_counts"] == {"stock": 100}
    assert result["selected_counts"] == {"stock": 1}
    assert result["estimated_daily_requests"] == 30


async def test_preflight_uses_symbol_whitelist_in_requested_order(tmp_path: Path):
    repo = Repository(f"sqlite:///{tmp_path / 'bridge.db'}")
    repo.create_tables()
    repo.upsert_instrument({"symbol": "AAPL"}, "stock")
    repo.upsert_instrument({"symbol": "BTCUSD"}, "crypto")
    repo.upsert_instrument({"symbol": "EURUSD"}, "forex")
    service = SyncService(
        FakeFMP(),
        repo,
        FakeWeKnora(),
        concurrency=1,
        bootstrap_limit=1,
        shard_size=10,
        sync_symbols=("EURUSD", "AAPL"),
    )

    result = await service.preflight()

    assert result["sync_symbols"] == ["EURUSD", "AAPL"]
    assert result["resolved_symbols"] == ["AAPL", "EURUSD"]
    assert result["missing_symbols"] == []
    assert result["selected_counts"] == {"forex": 1, "stock": 1}
    assert result["estimated_daily_requests"] == 56
    assert [instrument.symbol for instrument in service._selected_instruments()] == ["EURUSD", "AAPL"]


async def test_preflight_rejects_unknown_whitelist_symbol(tmp_path: Path):
    repo = Repository(f"sqlite:///{tmp_path / 'bridge.db'}")
    repo.create_tables()
    repo.upsert_instrument({"symbol": "AAPL"}, "stock")
    service = SyncService(
        FakeFMP(),
        repo,
        FakeWeKnora(),
        concurrency=1,
        bootstrap_limit=0,
        shard_size=10,
        sync_symbols=("AAPL", "UNKNOWN"),
    )

    with pytest.raises(RuntimeError, match="UNKNOWN"):
        await service.preflight()
