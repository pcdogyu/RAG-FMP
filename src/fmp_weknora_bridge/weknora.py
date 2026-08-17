from __future__ import annotations

from typing import Any

import httpx


class WeKnoraError(RuntimeError):
    pass


class WeKnoraClient:
    """Adapter for WeKnora's documented manual-knowledge create/update endpoints."""

    def __init__(self, base_url: str, api_key: str, knowledge_base_id: str):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.knowledge_base_id = knowledge_base_id
        self.client = httpx.AsyncClient(timeout=45)

    async def close(self) -> None:
        await self.client.aclose()

    @property
    def configured(self) -> bool:
        return bool(self.api_key and self.knowledge_base_id)

    def _headers(self) -> dict[str, str]:
        return {"X-API-Key": self.api_key, "Content-Type": "application/json"}

    async def preflight(self) -> None:
        if not self.configured:
            raise WeKnoraError(
                "WEKNORA_API_KEY and WEKNORA_KNOWLEDGE_BASE_ID are required for synchronization"
            )
        response = await self.client.get(
            f"{self.base_url}/api/v1/knowledge-bases/{self.knowledge_base_id}",
            headers=self._headers(),
        )
        if response.status_code >= 400:
            raise WeKnoraError(
                f"WeKnora knowledge base validation failed: {response.status_code} {response.text[:300]}"
            )

    async def upsert_markdown(self, title: str, content: str, knowledge_id: str = "") -> str:
        if not self.configured:
            raise WeKnoraError("WeKnora synchronization is not configured")
        payload: dict[str, Any] = {
            "title": title,
            "content": content,
            "status": "completed",
            "channel": "fmp",
        }
        if knowledge_id:
            response = await self.client.put(
                f"{self.base_url}/api/v1/knowledge/manual/{knowledge_id}",
                json=payload,
                headers=self._headers(),
            )
        else:
            response = await self.client.post(
                f"{self.base_url}/api/v1/knowledge-bases/{self.knowledge_base_id}/knowledge/manual",
                json=payload,
                headers=self._headers(),
            )
        if response.status_code >= 400:
            raise WeKnoraError(
                f"WeKnora document write failed: {response.status_code} {response.text[:300]}"
            )
        data = response.json().get("data", {})
        result_id = data.get("id") or knowledge_id
        if not result_id:
            raise WeKnoraError("WeKnora response did not contain a knowledge ID")
        return str(result_id)
