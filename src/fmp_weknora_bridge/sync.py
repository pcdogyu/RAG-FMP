from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

from .fmp import ASSET_TYPES, FMPClient
from .observability import SYNC_DOCUMENTS, SYNC_DURATION, SYNC_LAST_SUCCESS
from .research import build_research_markdown, content_hash
from .storage import Repository
from .weknora import WeKnoraClient


class SyncService:
    def __init__(
        self,
        fmp: FMPClient,
        repository: Repository,
        weknora: WeKnoraClient,
        *,
        concurrency: int,
        bootstrap_limit: int,
        shard_size: int,
    ):
        self.fmp = fmp
        self.repository = repository
        self.weknora = weknora
        self.semaphore = asyncio.Semaphore(concurrency)
        self.bootstrap_limit = bootstrap_limit
        self.shard_size = shard_size
        self._catalog_lock = asyncio.Lock()
        self._snapshot_lock = asyncio.Lock()

    async def preflight(self) -> dict[str, object]:
        checks: dict[str, object] = {"fmp": "pending", "weknora": "pending"}
        await self.fmp.cached_request("quote", {"symbol": "AAPL"}, ttl_seconds=1)
        checks["fmp"] = "ok"
        await self.weknora.preflight()
        checks["weknora"] = "ok"
        counts = self.repository.instrument_counts()
        # Starter uses the allowed single-symbol quote endpoint, not batch-quote.
        quote_calls = sum(counts.values())
        daily_research_calls = sum(
            count * (6 if asset_type in {"stock", "etf"} else 2)
            for asset_type, count in counts.items()
        )
        estimated = quote_calls * 24 + daily_research_calls
        checks["catalog_counts"] = counts
        checks["estimated_daily_requests"] = estimated
        if estimated and estimated > self.fmp.limiter.max_per_day:
            raise RuntimeError(
                f"configured FMP daily budget ({self.fmp.limiter.max_per_day}) is below the conservative "
                f"full-market estimate ({estimated}); reduce universe or raise the budget before enabling sync"
            )
        checks["checked_at"] = datetime.now(UTC).isoformat()
        return checks

    async def refresh_catalog(self) -> dict[str, int]:
        if self._catalog_lock.locked():
            return {"skipped": 1}
        async with self._catalog_lock:
            run = self.repository.start_run("catalog")
            counts: dict[str, int] = {}
            try:
                for asset_type in sorted(ASSET_TYPES):
                    catalog = await self.fmp.catalog(asset_type)
                    count = 0
                    for item in catalog if isinstance(catalog, list) else []:
                        try:
                            self.repository.upsert_instrument(item, asset_type)
                            count += 1
                        except ValueError:
                            continue
                    counts[asset_type] = count
                self.repository.finish_run(
                    run.id, processed=sum(counts.values()), written=sum(counts.values())
                )
                return counts
            except Exception as exc:
                self.repository.finish_run(
                    run.id, processed=sum(counts.values()), written=0, error=str(exc)
                )
                raise

    async def run_hourly_snapshot(self) -> dict[str, int]:
        # Do not permit the expensive global job before validating FMP access,
        # WeKnora write access, and the configured daily request budget.
        await self.preflight()
        if self._snapshot_lock.locked():
            return {"skipped": 1}
        async with self._snapshot_lock:
            with SYNC_DURATION.labels(name="hourly").time():
                run = self.repository.start_run("hourly")
                instruments = self.repository.list_instruments(limit=self.bootstrap_limit)
                processed = written = 0
                try:
                    for start in range(0, len(instruments), self.shard_size):
                        shard = instruments[start : start + self.shard_size]
                        tasks = [
                            self._sync_instrument(instrument.symbol, instrument.asset_type)
                            for instrument in shard
                        ]
                        for outcome in await asyncio.gather(*tasks, return_exceptions=True):
                            processed += 1
                            if outcome is True:
                                written += 1
                            elif isinstance(outcome, Exception):
                                SYNC_DOCUMENTS.labels(result="failed").inc()
                    self.repository.finish_run(run.id, processed=processed, written=written)
                    SYNC_LAST_SUCCESS.set_to_current_time()
                    return {"processed": processed, "written": written}
                except Exception as exc:
                    self.repository.finish_run(
                        run.id, processed=processed, written=written, error=str(exc)
                    )
                    raise

    async def _sync_instrument(self, symbol: str, asset_type: str) -> bool:
        async with self.semaphore:
            quotes = await self.fmp.quotes([symbol], asset_type)
            quote = quotes[0] if quotes else {"symbol": symbol}
            state = self.repository.document_state(symbol, asset_type)
            last_fundamentals = state.get("fundamentals_refreshed_at")
            refresh_fundamentals = (
                not isinstance(last_fundamentals, datetime)
                or (datetime.utcnow() - last_fundamentals).total_seconds() >= 86400
            )
            if asset_type in {"stock", "etf"}:
                if refresh_fundamentals:
                    research = await self.fmp.company_research(symbol)
                else:
                    research = json.loads(str(state.get("research_payload") or "{}"))
            else:
                research = {
                    "profile": [],
                    "income_statement": [],
                    "key_metrics": [],
                    "ratios": [],
                    "news": await self.fmp.market_news(asset_type, [symbol], 10),
                }
            markdown = build_research_markdown(symbol, asset_type, quote, research)
            digest = content_hash(markdown)
            changed, knowledge_id = self.repository.needs_write(symbol, asset_type, digest)
            if not changed:
                SYNC_DOCUMENTS.labels(result="unchanged").inc()
                return False
            knowledge_id = await self.weknora.upsert_markdown(
                title=f"FMP | {symbol} | {asset_type}", content=markdown, knowledge_id=knowledge_id
            )
            self.repository.save_document(
                symbol,
                asset_type,
                digest,
                knowledge_id,
                json.dumps(research, sort_keys=True, default=str),
                datetime.utcnow() if refresh_fundamentals else last_fundamentals,
            )
            SYNC_DOCUMENTS.labels(result="written").inc()
            return True
