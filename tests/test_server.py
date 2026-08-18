from starlette.testclient import TestClient

from fmp_weknora_bridge.server import create_app
from fmp_weknora_bridge.settings import Settings


def test_health_is_public_and_ready_requires_fmp_key(tmp_path):
    app = create_app(
        Settings(database_url=f"sqlite:///{tmp_path / 'bridge.db'}", sync_enabled=False)
    )
    with TestClient(app) as client:
        assert client.get("/health").json() == {"status": "ok"}
        assert client.get("/ready").status_code == 503
        assert client.get("/admin/runs").status_code == 401


def test_disabled_sync_rejects_manual_write_endpoints(tmp_path):
    app = create_app(
        Settings(
            database_url=f"sqlite:///{tmp_path / 'bridge.db'}",
            mcp_bearer_token="test-token",
            sync_enabled=False,
        )
    )
    headers = {"Authorization": "Bearer test-token"}
    with TestClient(app) as client:
        for endpoint in ("/admin/sync/catalog", "/admin/sync/hourly"):
            response = client.post(endpoint, headers=headers)
            assert response.status_code == 409
            assert response.json() == {
                "detail": "Synchronization is disabled by SYNC_ENABLED=false"
            }


def test_sync_symbols_are_normalized():
    settings = Settings(
        sync_symbols=" aapl, BTCUSD, AAPL ,, eurusd ",
        sync_universes=" crypto, NASDAQ, crypto, forex_g10 ",
    )

    assert settings.sync_enabled is False
    assert settings.sync_symbols == "AAPL,BTCUSD,EURUSD"
    assert settings.sync_symbol_list == ("AAPL", "BTCUSD", "EURUSD")
    assert settings.sync_universes == "crypto,nasdaq,forex_g10"
    assert settings.sync_universe_list == ("crypto", "nasdaq", "forex_g10")
