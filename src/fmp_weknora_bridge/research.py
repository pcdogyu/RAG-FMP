from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any


def build_research_markdown(
    symbol: str, asset_type: str, quote: dict[str, Any], research: dict[str, Any]
) -> str:
    """Create a stable, compact Markdown document suitable for RAG ingestion."""
    profile = _first(research.get("profile"))
    income = _first(research.get("income_statement"))
    metrics = _first(research.get("key_metrics"))
    ratios = _first(research.get("ratios"))
    news = research.get("news") or []
    now = datetime.now(UTC).replace(microsecond=0).isoformat()
    title = profile.get("companyName") or quote.get("name") or symbol
    lines = [
        f"# {title} ({symbol})",
        "",
        "## 数据元信息",
        f"- 资产类别: {asset_type}",
        f"- 同步时间: {now}",
        "- 数据源: Financial Modeling Prep (FMP)",
        "",
        "## 最新行情",
        f"- 价格: {_value(quote, 'price')}",
        f"- 变动: {_value(quote, 'change')} ({_value(quote, 'changesPercentage')}%)",
        f"- 成交量: {_value(quote, 'volume')}",
        f"- 市值: {_value(quote, 'marketCap')}",
        "",
        "## 公司与基本面",
        f"- 行业: {_value(profile, 'industry')}",
        f"- 板块: {_value(profile, 'sector')}",
        f"- 国家/交易所: {_value(profile, 'country')} / {_value(profile, 'exchangeShortName')}",
        f"- 最新季度收入: {_value(income, 'revenue')}",
        f"- 最新季度净利润: {_value(income, 'netIncome')}",
        f"- 年度 P/E: {_value(metrics, 'peRatio')}",
        f"- 年度自由现金流/股: {_value(metrics, 'freeCashFlowPerShare')}",
        f"- 流动比率: {_value(ratios, 'currentRatio')}",
        f"- 净利率: {_value(ratios, 'netProfitMargin')}",
    ]
    if news:
        lines += ["", "## 最新新闻"]
        for item in news[:5]:
            headline = str(item.get("title") or item.get("text") or "")
            url = str(item.get("url") or "")
            published = str(item.get("publishedDate") or item.get("date") or "")
            lines.append(
                f"- {published}: [{headline}]({url})" if url else f"- {published}: {headline}"
            )
    lines += [
        "",
        "## FMP 来源端点",
        "- /stable/profile",
        "- /stable/quote 或 /stable/batch-quote",
        "- /stable/income-statement",
        "- /stable/key-metrics",
        "- /stable/ratios",
        "- /stable/news/stock",
    ]
    return "\n".join(lines) + "\n"


def content_hash(markdown: str) -> str:
    # Do not let the generated sync timestamp alone create a new document revision.
    normalized = "\n".join(
        line for line in markdown.splitlines() if not line.startswith("- 同步时间:")
    )
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def safe_structured_result(data: Any, endpoint: str) -> dict[str, Any]:
    return {
        "source": {"provider": "Financial Modeling Prep", "endpoint": endpoint},
        "retrieved_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "data": data,
    }


def _first(value: Any) -> dict[str, Any]:
    if isinstance(value, list):
        return value[0] if value and isinstance(value[0], dict) else {}
    return value if isinstance(value, dict) else {}


def _value(item: dict[str, Any], key: str) -> str:
    value = item.get(key)
    return "N/A" if value is None or value == "" else str(value)
