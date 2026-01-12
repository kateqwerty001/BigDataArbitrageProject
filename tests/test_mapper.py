from jobs.batch_bbo_job import BatchBBOJob

def test_batch_mapper_encoding():
    """
    Verifies that the _mapper static method correctly converts
    dictionary data
    """
    mock_row = {
        "rowkey": "ETH|12345",
        "readable_time": "2026-01-12 12:00:00",
        "symbol": "ETH",
        "direction": "Binance->Kraken",
        "profit_usdt": 12.50,
        "roi_pct": 0.45,
        "buy_price": 2000.0,
        "sell_price": 2010.0,
        "trade_qty": 1.5
    }
    row_key, data = BatchBBOJob._mapper(mock_row)
    assert isinstance(row_key, bytes)
    assert row_key == b"ETH|12345"
    assert data[b"info:sym"] == b"ETH"
    assert data[b"info:dir"] == b"Binance->Kraken"
    assert data[b"trade_info:buy"] == b"2000.0"
    assert data[b"trade_info:sell"] == b"2010.0"
    assert data[b"profit_metrics:profit"] == b"12.5"