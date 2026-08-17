from __future__ import annotations

import asyncio
import json
from collections import Counter
from datetime import UTC, datetime

from .fmp import ASSET_TYPES, FMPClient
from .models import Instrument
from .observability import SYNC_DOCUMENTS, SYNC_DURATION, SYNC_LAST_SUCCESS
from .research import build_research_markdown, content_hash
from .storage import Repository
from .weknora import WeKnoraClient

G10_CURRENCIES = frozenset({"USD", "EUR", "JPY", "GBP", "CHF", "CAD", "AUD", "NZD", "SEK", "NOK"})
ROTATION_CURSOR_NAME = "market-universe-v1"


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
        sync_symbols: tuple[str, ...] = (),
        sync_universes: tuple[str, ...] = (),
        rotation_batch_size: int = 1000,
    ):
        self.fmp = fmp
        self.repository = repository
        self.weknora = weknora
        self.semaphore = asyncio.Semaphore(concurrency)
        self.bootstrap_limit = bootstrap_limit
        self.shard_size = shard_size
        self.sync_symbols = sync_symbols
        self.sync_universes = sync_universes
        self.rotation_batch_size = rotation_batch_size
        self._catalog_lock = asyncio.Lock()
        self._snapshot_lock = asyncio.Lock()

    async def preflight(self) -> dict[str, object]:
        checks: dict[str, object] = {"fmp": "pending", "weknora": "pending"}
        await self.fmp.cached_request("quote", {"symbol": "AAPL"}, ttl_seconds=1)
        checks["fmp"] = "ok"
        await self.weknora.preflight()
        checks["weknora"] = "ok"
        catalog_counts = self.repository.instrument_counts()
        selected_instruments = self._selected_instruments()
        if self.sync_symbols:
            resolved_symbols = {instrument.symbol.upper() for instrument in selected_instruments}
            missing_symbols = [symbol for symbol in self.sync_symbols if symbol not in resolved_symbols]
            checks["sync_symbols"] = list(self.sync_symbols)
            checks["resolved_symbols"] = sorted(resolved_symbols)
            checks["missing_symbols"] = missing_symbols
            if missing_symbols:
                raise RuntimeError(
                    "SYNC_SYMBOLS contains symbols absent from the active catalog: "
                    + ", ".join(missing_symbols)
                    + "; run catalog synchronization or correct the manual symbols"
                )

        selected_counts = dict(Counter(item.asset_type for item in selected_instruments))
        checks["catalog_counts"] = catalog_counts
        checks["selected_counts"] = selected_counts
        checks["bootstrap_limit"] = self.bootstrap_limit

        if self.sync_universes:
            universe_counts = self._universe_counts()
            if "nasdaq" in self.sync_universes and not universe_counts["nasdaq"]:
                raise RuntimeError(
                    "NASDAQ universe is configured but no NASDAQ constituents are available in the "
                    "catalog. Refresh the catalog with FMP access to company-screener, or remove "
                    "nasdaq from SYNC_UNIVERSES."
                )
            hourly_batch = min(self.rotation_batch_size, len(selected_instruments))
            daily_rotation = self._planned_rotation_instruments(hours=24)
            daily_counts = Counter(item.asset_type for item in daily_rotation)
            fundamental_targets = {
                item.id for item in daily_rotation if item.asset_type in {"stock", "etf"}
            }
            # Quotes are fetched on every rotation visit. Crypto/forex also fetch
            # market news on every visit; stock/ETF research has five requests only
            # when a symbol is first encountered in the 24-hour rotation window.
            estimated = (
                len(daily_rotation)
                + daily_counts["crypto"]
                + daily_counts["forex"]
                + len(fundamental_targets) * 5
            )
            checks.update(
                {
                    "sync_universes": list(self.sync_universes),
                    "universe_counts": universe_counts,
                    "effective_universe_count": len(selected_instruments),
                    "hourly_rotation_batch_size": hourly_batch,
                    "full_coverage_hours": _ceil_div(len(selected_instruments), hourly_batch),
                    "estimated_daily_rotating_visits": len(daily_rotation),
                    "estimated_daily_rotation_counts": dict(daily_counts),
                    "estimated_daily_fundamental_targets": len(fundamental_targets),
                }
            )
        else:
            # Existing manual/bootstrap behavior remains stable when no dynamic
            # market universe is configured.
            selected_counts = (
                selected_counts if self.sync_symbols or self.bootstrap_limit else catalog_counts
            )
            checks["selected_counts"] = selected_counts
            quote_calls = sum(selected_counts.values())
            daily_research_calls = sum(
                count * (6 if asset_type in {"stock", "etf"} else 2)
                for asset_type, count in selected_counts.items()
            )
            estimated = quote_calls * 24 + daily_research_calls

        checks["estimated_daily_requests"] = estimated
        checks["daily_request_budget"] = self.fmp.limiter.max_per_day
        if estimated and estimated > self.fmp.limiter.max_per_day:
            raise RuntimeError(
                f"configured FMP daily budget ({self.fmp.limiter.max_per_day}) is below the conservative "
                f"sync estimate ({estimated}); reduce universe or raise the budget before enabling sync"
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
                    counts[asset_type] = self._upsert_catalog(catalog, asset_type)
                # stock-list does not reliably carry exchange information for this
                # subscription, so NASDAQ listings are sourced from the dedicated
                # Starter-authorized exchange-filtered company screener.
                counts["nasdaq"] = self._upsert_catalog(
                    await self.fmp.nasdaq_stocks(), "stock", exchange_override="NASDAQ"
                )
                self.repository.finish_run(
                    run.id, processed=sum(counts.values()), written=sum(counts.values())
                )
                return counts
            except Exception as exc:
                self.repository.finish_run(
                    run.id, processed=sum(counts.values()), written=0, error=str(exc)
                )
                raise

    def _upsert_catalog(
        self, catalog: object, asset_type: str, *, exchange_override: str | None = None
    ) -> int:
        count = 0
        for item in catalog if isinstance(catalog, list) else []:
            try:
                self.repository.upsert_instrument(
                    item, asset_type, exchange_override=exchange_override
                )
                count += 1
            except ValueError:
                continue
        return count

    async def run_hourly_snapshot(self) -> dict[str, int]:
        await self.preflight()
        if self._snapshot_lock.locked():
            return {"skipped": 1}
        async with self._snapshot_lock:
            with SYNC_DURATION.labels(name="hourly").time():
                run = self.repository.start_run("hourly")
                instruments, rotation_next_position = self._hourly_instruments()
                processed = written = failed = 0
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
                                failed += 1
                                SYNC_DOCUMENTS.labels(result="failed").inc()
                    if failed:
                        self.repository.finish_run(
                            run.id,
                            processed=processed,
                            written=written,
                            error=f"{failed} instrument(s) failed; rotation cursor was not advanced",
                        )
                    else:
                        self.repository.finish_run(run.id, processed=processed, written=written)
                        self._advance_rotation_cursor(rotation_next_position)
                        SYNC_LAST_SUCCESS.set_to_current_time()
                    result = {"processed": processed, "written": written}
                    if failed:
                        result["failed"] = failed
                    return result
                except Exception as exc:
                    self.repository.finish_run(
                        run.id, processed=processed, written=written, error=str(exc)
                    )
                    raise

    def _selected_instruments(self) -> list[Instrument]:
        if not self.sync_universes:
            if self.sync_symbols:
                return self.repository.list_instruments(symbols=self.sync_symbols)
            return self.repository.list_instruments(limit=self.bootstrap_limit)

        all_instruments = self.repository.list_instruments()
        by_id: dict[int, Instrument] = {}
        for instrument in self.repository.list_instruments(symbols=self.sync_symbols):
            by_id[instrument.id] = instrument
        for instrument in all_instruments:
            if "crypto" in self.sync_universes and instrument.asset_type == "crypto":
                by_id[instrument.id] = instrument
            elif "nasdaq" in self.sync_universes and (
                instrument.asset_type == "stock" and instrument.exchange.upper() == "NASDAQ"
            ):
                by_id[instrument.id] = instrument
            elif "forex_g10" in self.sync_universes and _is_g10_forex(instrument):
                by_id[instrument.id] = instrument
        return [by_id[instrument_id] for instrument_id in sorted(by_id)]

    def _universe_counts(self) -> dict[str, int]:
        instruments = self.repository.list_instruments()
        return {
            "crypto": sum(item.asset_type == "crypto" for item in instruments)
            if "crypto" in self.sync_universes
            else 0,
            "nasdaq": sum(
                item.asset_type == "stock" and item.exchange.upper() == "NASDAQ"
                for item in instruments
            )
            if "nasdaq" in self.sync_universes
            else 0,
            "forex_g10": sum(_is_g10_forex(item) for item in instruments)
            if "forex_g10" in self.sync_universes
            else 0,
        }

    def _hourly_instruments(self) -> tuple[list[Instrument], int | None]:
        instruments = self._selected_instruments()
        if not self.sync_universes or not instruments:
            return instruments, None
        instruments = _interleave_by_asset_type(instruments)
        limit = min(self.rotation_batch_size, len(instruments))
        start = self.repository.get_sync_cursor(ROTATION_CURSOR_NAME) % len(instruments)
        return (instruments[start:] + instruments[:start])[:limit], (start + limit) % len(instruments)

    def _planned_rotation_instruments(self, *, hours: int) -> list[Instrument]:
        instruments = self._selected_instruments()
        if not instruments or not self.sync_universes:
            return instruments
        instruments = _interleave_by_asset_type(instruments)
        batch_size = min(self.rotation_batch_size, len(instruments))
        position = self.repository.get_sync_cursor(ROTATION_CURSOR_NAME) % len(instruments)
        planned: list[Instrument] = []
        for _ in range(hours):
            planned.extend((instruments[position:] + instruments[:position])[:batch_size])
            position = (position + batch_size) % len(instruments)
        return planned

    def _advance_rotation_cursor(self, next_position: int | None) -> None:
        if self.sync_universes and next_position is not None:
            self.repository.set_sync_cursor(ROTATION_CURSOR_NAME, next_position)

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


def _is_g10_forex(instrument: Instrument) -> bool:
    if instrument.asset_type != "forex":
        return False
    symbol = instrument.symbol.upper()
    return (
        len(symbol) == 6
        and symbol.isalpha()
        and symbol[:3] in G10_CURRENCIES
        and symbol[3:] in G10_CURRENCIES
        and symbol[:3] != symbol[3:]
    )


def _ceil_div(value: int, divisor: int) -> int:
    return (value + divisor - 1) // divisor if divisor else 0


def _interleave_by_asset_type(instruments: list[Instrument]) -> list[Instrument]:
    """Produce a stable order that gives each requested market a turn per batch."""
    groups: dict[str, list[Instrument]] = {}
    for instrument in instruments:
        groups.setdefault(instrument.asset_type, []).append(instrument)
    for group in groups.values():
        group.sort(key=lambda item: (item.symbol, item.id))
    ordered: list[Instrument] = []
    types = sorted(groups)
    for index in range(max((len(group) for group in groups.values()), default=0)):
        for asset_type in types:
            if index < len(groups[asset_type]):
                ordered.append(groups[asset_type][index])
    return ordered
