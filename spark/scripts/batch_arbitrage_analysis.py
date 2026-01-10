"""
Big Data Project - Batch Layer Analysis
Description: Calculates arbitrage opportunities and stores them in HBase.
"""
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, lit, when, least, from_unixtime, concat

def main():
    # -------------------------------------------------------------------------
    # 1. Initialize Spark Session
    # -------------------------------------------------------------------------
    spark = SparkSession.builder \
        .appName("CryptoArbitrageBatchToHBase") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(">>> [1/7] Spark Session initialized.")

    # -------------------------------------------------------------------------
    # 2. Determine Target Date
    # -------------------------------------------------------------------------
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        target_date = datetime.now().strftime('%Y-%m-%d')
    
    print(f">>> Target Date: {target_date}")

    # -------------------------------------------------------------------------
    # 3. Extract & Clean Data
    # -------------------------------------------------------------------------
    print(f">>> [2/7] Reading Hive table 'raw_bbo_json'...")
    df_raw = spark.sql(f"SELECT line FROM raw_bbo_json WHERE dt='{target_date}'")

    if df_raw.rdd.isEmpty():
        print(f"!!! WARNING: No data found for {target_date}. Exiting...")
        spark.stop()
        return

    # -------------------------------------------------------------------------
    # 4. Parse JSON
    # -------------------------------------------------------------------------
    print(">>> [3/7] Parsing JSON (Prices & Quantities)...")
    df_parsed = df_raw.select(
        get_json_object(col("line"), "$.exchange").alias("exchange"),
        get_json_object(col("line"), "$.symbol").alias("symbol"),
        get_json_object(col("line"), "$.bid_price").cast("double").alias("bid_price"),
        get_json_object(col("line"), "$.ask_price").cast("double").alias("ask_price"),
        get_json_object(col("line"), "$.bid_qty").cast("double").alias("bid_qty"),
        get_json_object(col("line"), "$.ask_qty").cast("double").alias("ask_qty"),
        get_json_object(col("line"), "$.ts_ingest_ms").cast("long").alias("timestamp")
    )

    df_clean = df_parsed.filter(col("bid_price").isNotNull()) \
                        .dropDuplicates(["exchange", "symbol", "timestamp"])

    # -------------------------------------------------------------------------
    # 5. Separate & Align Streams
    # -------------------------------------------------------------------------
    print(">>> [4/7] Aligning timestamps...")
    
    df_binance = df_clean.filter(col("exchange") == "binance") \
        .select(
            col("symbol"),
            col("timestamp").alias("bin_time"),
            col("bid_price").alias("bin_bid"), col("bid_qty").alias("bin_bid_qty"),
            col("ask_price").alias("bin_ask"), col("ask_qty").alias("bin_ask_qty")
        ).withColumn("time_sec", (col("bin_time") / 1000).cast("long"))

    df_kraken = df_clean.filter(col("exchange") == "kraken") \
        .select(
            col("symbol"),
            col("timestamp").alias("krk_time"),
            col("bid_price").alias("krk_bid"), col("bid_qty").alias("krk_bid_qty"),
            col("ask_price").alias("krk_ask"), col("ask_qty").alias("krk_ask_qty")
        ).withColumn("time_sec", (col("krk_time") / 1000).cast("long"))

    df_joined = df_binance.join(df_kraken, ["symbol", "time_sec"], "inner")

    # -------------------------------------------------------------------------
    # 6. Analysis: Calculate Profits
    # -------------------------------------------------------------------------
    print(">>> [5/7] Calculating profits...")
    FEE_RATE = 0.0 

    df_calc = df_joined.withColumn(
        "spread_bin_to_krk",
        ((col("krk_bid") - col("bin_ask")) / col("bin_ask")) - FEE_RATE
    ).withColumn(
        "spread_krk_to_bin",
        ((col("bin_bid") - col("krk_ask")) / col("krk_ask")) - FEE_RATE
    )

    df_profitable = df_calc.filter((col("spread_bin_to_krk") > 0) | (col("spread_krk_to_bin") > 0))

    is_bin_to_krk = col("spread_bin_to_krk") > col("spread_krk_to_bin")

    df_final = df_profitable.select(
        from_unixtime(col("time_sec")).alias("readable_time"),
        col("symbol"),
        when(is_bin_to_krk, col("spread_bin_to_krk") * 100).otherwise(col("spread_krk_to_bin") * 100).alias("roi_pct"),
        when(is_bin_to_krk, least(col("bin_ask_qty"), col("krk_bid_qty"))).otherwise(least(col("krk_ask_qty"), col("bin_bid_qty"))).alias("trade_qty"),
        when(is_bin_to_krk, col("bin_ask")).otherwise(col("krk_ask")).alias("buy_price"),
        when(is_bin_to_krk, col("krk_bid")).otherwise(col("bin_bid")).alias("sell_price"),
        when(is_bin_to_krk, lit("Binance -> Kraken")).otherwise(lit("Kraken -> Binance")).alias("direction")
    )

    total_fees_cost = (col("buy_price") + col("sell_price")) * col("trade_qty") * (FEE_RATE / 2)
    df_final = df_final.withColumn("profit_usdt", ((col("sell_price") - col("buy_price")) * col("trade_qty")) - total_fees_cost)

    # -------------------------------------------------------------------------
    # 7. Output (Saving to HBase or Hive if fails)
    # -------------------------------------------------------------------------
    print(">>> [6/7] Saving results to HBase...")

    # Define RowKey
    df_hbase = df_final.withColumn("rowkey", concat(col("symbol"), lit("_"), col("readable_time")))

    # HBase Configuration
    catalog = ''.join("""{
        "table":{"namespace":"default", "name":"arbitrage_results"},
        "rowkey":"key",
        "columns":{
            "rowkey":        {"cf":"rowkey",         "col":"key",    "type":"string"},
            "readable_time": {"cf":"time",           "col":"val",    "type":"string"},
            "symbol":        {"cf":"info",           "col":"sym",    "type":"string"},
            "direction":     {"cf":"info",           "col":"dir",    "type":"string"},
            "buy_price":     {"cf":"trade_info",     "col":"buy",    "type":"double"},
            "sell_price":    {"cf":"trade_info",     "col":"sell",   "type":"double"},
            "trade_qty":     {"cf":"trade_info",     "col":"qty",    "type":"double"},
            "roi_pct":       {"cf":"profit_metrics", "col":"roi",    "type":"double"},
            "profit_usdt":   {"cf":"profit_metrics", "col":"profit", "type":"double"}
        }
    }""".split())

    try:
        df_hbase.write \
            .options(catalog=catalog) \
            .format("org.apache.spark.sql.execution.datasources.hbase") \
            .mode("append") \
            .save()
        print(">>> SUCCESS: Results saved to HBase table 'arbitrage_results'.")
    except Exception as e:
        print("!!! HBase Connector not found in classpath. Saving to Hive as fallback...")
        df_hbase.write.mode("append").saveAsTable("arbitrage_batch_results")

    # -------------------------------------------------------------------------
    # 8. Final Summary
    # -------------------------------------------------------------------------
    total_count = df_hbase.count()
    print("-" * 100)
    print(f"BATCH ANALYSIS COMPLETE. Total records: {total_count}")
    print("-" * 100)
    
    if total_count > 0:
        df_hbase.select("readable_time", "symbol", "direction", "roi_pct", "profit_usdt", "sell_price", "buy_price") \
                .orderBy(col("profit_usdt").desc()).show(10, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()