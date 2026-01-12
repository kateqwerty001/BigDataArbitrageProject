import sys
import os
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql.functions import (
    col, get_json_object, from_json, lit, concat, from_unixtime, to_date,
    current_timestamp, udf, when
)
from lib.spark_base import SparkJobBase
from lib.config import Config
from lib.schemas import Schemas
from lib.calculator import ArbitrageCalculator
from lib.storage import HBaseWriter

class BatchDepthJob(SparkJobBase):
    def __init__(self):
        super().__init__("BatchDepth500")

    @staticmethod
    def _mapper(row):
        rk = row["rowkey"].encode("utf-8")
        def bstr(x): return str(x).encode("utf-8")

        data = {
            b"time:val": bstr(row["readable_time"]),
            b"info:sym": bstr(row["symbol"]),
            b"info:dir": bstr(row["direction"]),
            b"trade_info:cost_used_usdt": bstr(row["cost_used_usdt"]),
            b"trade_info:base_qty": bstr(row["base_qty"]),
            b"depth_metrics:vwap_buy": bstr(row["vwap_buy"]),
            b"depth_metrics:vwap_sell": bstr(row["vwap_sell"]),
            b"depth_metrics:levels_buy": bstr(row["levels_buy"]),
            b"depth_metrics:levels_sell": bstr(row["levels_sell"]),
            b"profit_metrics:roi_depth_500_pct": bstr(row["roi_depth_500_pct"]),
            b"profit_metrics:profit_depth_500_usdt": bstr(row["profit_depth_500_usdt"]),
        }
        return rk, data

    def run(self):
        target_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
        print(f">>> Target Date: {target_date}")

        # 1. Read from Hive
        try:
            df_raw = self.spark.sql(f"SELECT line FROM {Config.HIVE_TABLE_DEPTH} WHERE dt='{target_date}'")
        except Exception as e:
            print(f"!!! Error reading Hive: {e}")
            return

        if df_raw.rdd.isEmpty():
            print("!!! No data found in Hive for this date.")
            return

        # 2. Parse JSON
        df_depth = df_raw.select(
            get_json_object(col("line"), "$.exchange").alias("exchange"),
            get_json_object(col("line"), "$.symbol").alias("symbol"),
            get_json_object(col("line"), "$.ts_ingest_ms").cast("long").alias("timestamp"),
            from_json(get_json_object(col("line"), "$.asks"), Schemas.level_schema).alias("asks"),
            from_json(get_json_object(col("line"), "$.bids"), Schemas.level_schema).alias("bids"),
        ).filter(col("exchange").isNotNull()).withColumn("time_sec", (col("timestamp") / 1000).cast("long")) \
         .dropDuplicates(["exchange", "symbol", "time_sec"])

        # 3. Join
        b = df_depth.filter(col("exchange") == "binance").alias("b")
        k = df_depth.filter(col("exchange") == "kraken").alias("k")
        joined = b.join(k, (col("b.symbol") == col("k.symbol")) & (col("b.time_sec") == col("k.time_sec")))

        # 4. Calculations
        buy_udf = udf(ArbitrageCalculator.buy_with_quote, Schemas.buy_ret_schema)
        sell_udf = udf(ArbitrageCalculator.sell_base_for_quote, Schemas.sell_ret_schema)

        # Step 4a: Calculate Buys
        df_calc = joined.withColumn("b_buy", buy_udf(col("b.asks"), lit(Config.TRADE_QUOTE_USDT))) \
                        .withColumn("k_buy", buy_udf(col("k.asks"), lit(Config.TRADE_QUOTE_USDT)))

        # Step 4b: Calculate Sells
        df_calc = df_calc.withColumn("k_sell", sell_udf(col("k.bids"), col("b_buy.base_qty"))) \
                         .withColumn("b_sell", sell_udf(col("b.bids"), col("k_buy.base_qty")))

        # Step 4c: Logic
        p_b2k = (col("k_sell.revenue") * (1 - Config.FEE_RATE)) - (col("b_buy.cost_used") * (1 + Config.FEE_RATE))
        p_k2b = (col("b_sell.revenue") * (1 - Config.FEE_RATE)) - (col("k_buy.cost_used") * (1 + Config.FEE_RATE))
        
        roi_b2k = p_b2k / col("b_buy.cost_used")
        roi_k2b = p_k2b / col("k_buy.cost_used")

        is_b2k = p_b2k > p_k2b

        # 5. Select Final Columns (including levels_buy/sell for compatibility)
        df_out = df_calc.select(
            from_unixtime(col("b.time_sec")).alias("readable_time"),
            col("b.symbol").alias("symbol"),
            when(is_b2k, lit("Binance -> Kraken")).otherwise(lit("Kraken -> Binance")).alias("direction"),
            
            # Metrics
            when(is_b2k, col("b_buy.cost_used")).otherwise(col("k_buy.cost_used")).alias("cost_used_usdt"),
            when(is_b2k, col("b_buy.base_qty")).otherwise(col("k_buy.base_qty")).alias("base_qty"),
            when(is_b2k, col("b_buy.vwap_buy")).otherwise(col("k_buy.vwap_buy")).alias("vwap_buy"),
            when(is_b2k, col("k_sell.vwap_sell")).otherwise(col("b_sell.vwap_sell")).alias("vwap_sell"),
            
            # Added missing columns for Hive compatibility
            when(is_b2k, col("b_buy.levels_buy")).otherwise(col("k_buy.levels_buy")).alias("levels_buy"),
            when(is_b2k, col("k_sell.levels_sell")).otherwise(col("b_sell.levels_sell")).alias("levels_sell"),

            # Profit & ROI
            (when(is_b2k, roi_b2k).otherwise(roi_k2b) * 100.0).alias("roi_depth_500_pct"),
            when(is_b2k, p_b2k).otherwise(p_k2b).alias("profit_depth_500_usdt"),
            
            col("b.time_sec")
        ).filter(col("profit_depth_500_usdt") > 0) \
         .withColumn("rowkey", concat(col("symbol"), lit("|"), (col("time_sec") * 1000).cast("string")))

        # 6. Write to HBase
        print(">>> Writing Batch Results to HBase...")
        df_out.foreachPartition(
            HBaseWriter.get_partition_writer(Config.TABLE_BATCH_DEPTH, BatchDepthJob._mapper)
        )

        # 7. Write to Hive
        print(">>> Writing Batch Results to Hive ...")
        
        # Prepare DataFrame for Hive: Add missing meta-columns
        df_hive = df_out.withColumn("processed_at", current_timestamp()) \
                        .withColumn("dt", to_date(col("readable_time")))
        df_hive_final = df_hive.select(
            "rowkey", 
            "readable_time", 
            "symbol", 
            "direction", 
            "cost_used_usdt", 
            "base_qty", 
            "vwap_buy", 
            "vwap_sell", 
            "levels_buy", 
            "levels_sell", 
            "roi_depth_500_pct", 
            "profit_depth_500_usdt", 
            "processed_at", 
            "dt"
        )

        df_hive_final.write.mode("append").format("parquet").partitionBy("dt").saveAsTable("arbitrage_depth_batch")

        print("SUCCESS - HISOTRY SAVED TO HBASE AND HIVE - as 'arbitrage_depth_batch' table")

        self.stop()

if __name__ == "__main__":
    job = BatchDepthJob()
    job.run()