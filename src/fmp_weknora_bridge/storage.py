from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime

from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker

from .models import Base, Instrument, ResearchDocument, SyncRun


class Repository:
    def __init__(self, database_url: str):
        connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
        self.engine = create_engine(database_url, pool_pre_ping=True, connect_args=connect_args)
        self.sessions = sessionmaker(self.engine, expire_on_commit=False)

    def create_tables(self) -> None:
        Base.metadata.create_all(self.engine)

    @contextmanager
    def session(self):
        with self.sessions() as session:
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise

    def upsert_instrument(self, item: dict, asset_type: str) -> Instrument:
        symbol = str(item.get("symbol") or item.get("ticker") or "").upper().strip()
        if not symbol:
            raise ValueError("FMP catalog row has no symbol")
        source_hash = _hashable(item)
        with self.session() as session:
            entity = session.scalar(
                select(Instrument).where(
                    Instrument.symbol == symbol, Instrument.asset_type == asset_type
                )
            )
            if entity is None:
                entity = Instrument(symbol=symbol, asset_type=asset_type)
                session.add(entity)
            entity.name = str(item.get("name") or item.get("companyName") or "")[:512]
            entity.exchange = str(item.get("exchange") or item.get("exchangeShortName") or "")[:128]
            entity.source_hash = source_hash
            entity.active = "true"
            return entity

    def list_instruments(self, limit: int = 0) -> list[Instrument]:
        with self.session() as session:
            query = select(Instrument).where(Instrument.active == "true").order_by(Instrument.id)
            if limit:
                query = query.limit(limit)
            return list(session.scalars(query))

    def instrument_counts(self) -> dict[str, int]:
        with self.session() as session:
            rows = session.execute(
                select(Instrument.asset_type, func.count(Instrument.id))
                .where(Instrument.active == "true")
                .group_by(Instrument.asset_type)
            )
            return {str(asset_type): int(count) for asset_type, count in rows}

    def start_run(self, name: str) -> SyncRun:
        with self.session() as session:
            run = SyncRun(name=name)
            session.add(run)
            session.flush()
            return run

    def finish_run(self, run_id: int, *, processed: int, written: int, error: str = "") -> None:
        with self.session() as session:
            run = session.get(SyncRun, run_id)
            if run is None:
                return
            run.status = "failed" if error else "completed"
            run.finished_at = datetime.utcnow()
            run.processed = processed
            run.written = written
            run.error = error[:4000]

    def latest_runs(self, limit: int = 10) -> list[SyncRun]:
        with self.session() as session:
            return list(session.scalars(select(SyncRun).order_by(SyncRun.id.desc()).limit(limit)))

    def needs_write(self, symbol: str, asset_type: str, content_hash: str) -> tuple[bool, str]:
        with self.session() as session:
            doc = session.scalar(
                select(ResearchDocument).where(
                    ResearchDocument.symbol == symbol, ResearchDocument.asset_type == asset_type
                )
            )
            return (
                doc is None or doc.content_hash != content_hash,
                doc.weknora_knowledge_id if doc else "",
            )

    def document_state(self, symbol: str, asset_type: str) -> dict[str, object]:
        with self.session() as session:
            doc = session.scalar(
                select(ResearchDocument).where(
                    ResearchDocument.symbol == symbol, ResearchDocument.asset_type == asset_type
                )
            )
            if doc is None:
                return {}
            return {
                "knowledge_id": doc.weknora_knowledge_id,
                "research_payload": doc.research_payload,
                "fundamentals_refreshed_at": doc.fundamentals_refreshed_at,
            }

    def save_document(
        self,
        symbol: str,
        asset_type: str,
        content_hash: str,
        knowledge_id: str,
        research_payload: str,
        fundamentals_refreshed_at: datetime | None,
    ) -> None:
        with self.session() as session:
            doc = session.scalar(
                select(ResearchDocument).where(
                    ResearchDocument.symbol == symbol, ResearchDocument.asset_type == asset_type
                )
            )
            if doc is None:
                doc = ResearchDocument(
                    symbol=symbol, asset_type=asset_type, content_hash=content_hash
                )
                session.add(doc)
            doc.content_hash = content_hash
            doc.weknora_knowledge_id = knowledge_id
            doc.research_payload = research_payload
            doc.fundamentals_refreshed_at = fundamentals_refreshed_at
            doc.last_synced_at = datetime.utcnow()


def _hashable(item: dict) -> str:
    import hashlib
    import json

    return hashlib.sha256(json.dumps(item, sort_keys=True, default=str).encode()).hexdigest()
