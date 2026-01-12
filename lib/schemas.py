from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, DoubleType, LongType
)
class Schemas:
    # --- Order Book Level Schema ---
    level_schema = ArrayType(
        StructType([
            StructField("price", StringType(), True),
            StructField("qty",   StringType(), True),
        ])
    )

    # --- Depth JSON Schema ---
    depth_schema = StructType([
        StructField("exchange", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("ts_ingest_ms", StringType(), True),
        StructField("asks", level_schema, True),
        StructField("bids", level_schema, True),
    ])

    # --- Trades JSON Schema ---
    trade_schema = StructType([
        StructField("exchange", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("ts_ingest_ms", StringType(), True),
        StructField("price", StringType(), True),
        StructField("qty", StringType(), True),
        StructField("side", StringType(), True), # 'buy' or 'sell'
    ])

    # --- BBO JSON Schema ---
    bbo_schema = StructType([
        StructField("exchange", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("ts_ingest_ms", StringType(), True),
        StructField("bid_price", StringType(), True),
        StructField("bid_qty", StringType(), True),
        StructField("ask_price", StringType(), True),
        StructField("ask_qty", StringType(), True),
    ])

    # --- UDF Return Schemas ---
    buy_ret_schema = StructType([
        StructField("base_qty",   DoubleType(), False),
        StructField("vwap_buy",   DoubleType(), False),
        StructField("cost_used",  DoubleType(), False),
        StructField("levels_buy", LongType(),   False),
    ])

    sell_ret_schema = StructType([
        StructField("revenue",     DoubleType(), False),
        StructField("vwap_sell",   DoubleType(), False),
        StructField("levels_sell", LongType(),   False),
    ])
