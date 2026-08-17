from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

FMP_REQUESTS = Counter("fmp_requests_total", "FMP requests", ["endpoint", "status"])
FMP_ERRORS = Counter("fmp_request_errors_total", "FMP request errors", ["endpoint", "reason"])
MCP_CALLS = Counter("fmp_mcp_tool_calls_total", "MCP tool calls", ["tool", "status"])
SYNC_DOCUMENTS = Counter("fmp_sync_documents_total", "Research documents", ["result"])
SYNC_LAST_SUCCESS = Gauge(
    "fmp_sync_last_success_timestamp", "Last successful synchronization timestamp"
)
SYNC_DURATION = Histogram("fmp_sync_duration_seconds", "Synchronization duration", ["name"])
