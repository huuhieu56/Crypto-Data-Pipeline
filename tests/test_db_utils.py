from unittest.mock import MagicMock

from utils import db_utils


def test_get_ch_client_returns_same_instance(monkeypatch):
    """get_ch_client should lazy-init once and reuse the same client."""
    client = MagicMock()
    get_client = MagicMock(return_value=client)

    monkeypatch.setattr(db_utils, "_client", None)
    monkeypatch.setattr(db_utils.clickhouse_connect, "get_client", get_client)

    c1 = db_utils.get_ch_client()
    c2 = db_utils.get_ch_client()

    assert c1 is client
    assert c2 is client
    get_client.assert_called_once()
    client.query.assert_called_once_with("SELECT 1")
