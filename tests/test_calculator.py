import pytest
from lib.calculator import ArbitrageCalculator

def test_bbo_arbitrage_binance_to_kraken(spark):
    """
    Direction: Buy Binance -> Sell Kraken.
    """
    # 1. Prepare Mock Data
    data = [
        {
            "symbol": "ETH/USDT",
            # Binance (Cheap Seller)
            "bin_ask": 1000.0, "bin_ask_q": 1.5,
            "bin_bid": 990.0,  "bin_bid_q": 1.0,
            # Kraken (Expensive Buyer)
            "krk_ask": 1110.0, "krk_ask_q": 2.0,
            "krk_bid": 1100.0, "krk_bid_q": 0.5,
        }
    ]
    df = spark.createDataFrame(data)
    result_df = ArbitrageCalculator.calculate_bbo_arbitrage(df, fee_rate=0.0)
    # Assertions
    assert result_df.count() == 1, "Should find 1 arbitrage opportunity"
    row = result_df.first()
    assert row["direction"] == "Binance->Kraken"
    # Volume check (min(1.5, 0.5) = 0.5)
    if "exec_volume" in row:
        assert row["exec_volume"] == 0.5 
    elif "trade_qty" in row:
        assert row["trade_qty"] == 0.5
    # Profit: (1100 - 1000) * 0.5 = 50.0 USDT
    assert row["profit_usdt"] == 50.0

def test_bbo_arbitrage_kraken_to_binance(spark):
    """
    Direction: Buy Kraken -> Sell Binance.
    """
    data = [
        {
            "symbol": "BTC/USDT",
            # Binance (Expensive Buyer)
            "bin_ask": 50100.0, "bin_ask_q": 1.0,
            "bin_bid": 50050.0, "bin_bid_q": 2.0, 
            # Kraken (Cheap Seller)
            "krk_ask": 50000.0, "krk_ask_q": 0.1,
            "krk_bid": 49900.0, "krk_bid_q": 1.0, 
        }
    ]
    df = spark.createDataFrame(data)
    result_df = ArbitrageCalculator.calculate_bbo_arbitrage(df, fee_rate=0.0)
    if result_df.count() == 0:
        pytest.fail("Result DataFrame is empty, expected 1 record")
    row = result_df.first()
    assert row["direction"] == "Kraken->Binance"
    assert round(row["profit_usdt"], 2) == 5.0

def test_no_arbitrage_scenario(spark):
    """
    Scenario: Prices are efficient (Ask > Bid everywhere).
    """
    data = [
        {
            "symbol": "SOL/USDT",
            "bin_ask": 100.0, "bin_ask_q": 10, "bin_bid": 99.0, "bin_bid_q": 10,
            "krk_ask": 101.0, "krk_ask_q": 10, "krk_bid": 98.0, "krk_bid_q": 10,
        }
    ]
    df = spark.createDataFrame(data)
    result_df = ArbitrageCalculator.calculate_bbo_arbitrage(df, fee_rate=0.0)
    assert result_df.count() == 0, "Should not find arbitrage when prices are normal"