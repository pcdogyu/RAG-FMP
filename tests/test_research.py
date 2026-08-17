from fmp_weknora_bridge.research import build_research_markdown, content_hash


def test_sync_timestamp_does_not_change_document_hash():
    research = {"profile": [{"companyName": "Apple", "industry": "Technology"}], "news": []}
    first = build_research_markdown("AAPL", "stock", {"price": 200}, research)
    second = build_research_markdown("AAPL", "stock", {"price": 200}, research)

    assert "# Apple (AAPL)" in first
    assert content_hash(first) == content_hash(second)
