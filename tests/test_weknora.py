import json

import respx
from httpx import Response

from fmp_weknora_bridge.weknora import WeKnoraClient


@respx.mock
async def test_create_manual_knowledge_uses_weknora_api():
    route = respx.post("http://weknora:8080/api/v1/knowledge-bases/kb-1/knowledge/manual").mock(
        return_value=Response(200, json={"data": {"id": "knowledge-1"}})
    )
    client = WeKnoraClient("http://weknora:8080", "wk-secret", "kb-1")
    try:
        knowledge_id = await client.upsert_markdown("FMP | AAPL", "# AAPL")
    finally:
        await client.close()

    assert knowledge_id == "knowledge-1"
    assert route.calls[0].request.headers["x-api-key"] == "wk-secret"
    assert json.loads(route.calls[0].request.content) == {"title": "FMP | AAPL", "content": "# AAPL"}
