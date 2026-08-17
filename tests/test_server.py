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


def test_sync_symbols_are_normalized():
    settings = Settings(sync_symbols=" aapl, BTCUSD, AAPL ,, eurusd ")

    assert settings.sync_symbols == "AAPL,BTCUSD,EURUSD"
    assert settings.sync_symbol_list == ("AAPL", "BTCUSD", "EURUSD")
