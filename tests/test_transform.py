from unittest.mock import MagicMock

from scripts import transform as transform_module


def test_get_indicator_watermarks_ignores_raw_zero_rows(monkeypatch):
    client = MagicMock()
    client.query.return_value.result_rows = [
        ("BTCUSDT", 1715731200000),
        ("ETHUSDT", 0),
    ]
    monkeypatch.setattr(transform_module, "get_ch_client", lambda: client)

    result = transform_module._get_indicator_watermarks(["BTCUSDT", "ETHUSDT"])

    assert result == {"BTCUSDT": 1715731200000}
    sql = client.query.call_args[0][0]
    assert "maxIf" in sql
    assert "isNotNull(rsi_14)" in sql
    assert "ifNull(rsi_14, 0) != 0" in sql
