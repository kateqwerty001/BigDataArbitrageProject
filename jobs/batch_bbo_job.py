import sys
import os
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql.functions import (
    col, get_json_object, lit, when, least, from_unixtime, concat, 
    current_timestamp, to_date
)
from lib.spark_base import SparkJobBase
from lib.config import Config
from lib.storage import HBaseWriter

class BatchBBOJob(SparkJobBase):
    def __init__(self):
        super().__init__("BatchBBO")

    @staticmethod
    def _mapper(row):
        rk = row["rowkey"].encode("utf-8")
        def bstr(x): return str(x).encode("utf-8")
        
        data = {
            # Basic Info
            b"time:val": bstr(row["readable_time"]),
            b"info:sym": bstr(row["symbol"]),
            b"info:dir": bstr(row["direction"]),
            
            # Trade Details
            b"trade_info:buy": bstr(row["buy_price"]),
            b"trade_info:sell": bstr(row["sell_price"]),
            b"trade_info:qty": bstr(row["trade_qty"]),
            
            # Metrics
            b"profit_metrics:profit": bstr(row["profit_usdt"]),
            b"profit_metrics:roi": bstr(row["roi_pct"]),
        }
        return rk, data

    def run(self):
        target_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
        print(f">>> Target Date: {target_date}")

        # 1. Read from Hive
        try:
            df_raw = self.spark.sql(f"SELECT line FROM {Config.HIVE_TABLE_BBO} WHERE dt='{target_date}'")
        except:
             print("!!! Hive table for BBO not found.")
             return

        if df_raw.rdd.isEmpty(): 
            print("!!! No BBO data found.")
            return

        # 2. Parse JSON
        df_parsed = df_raw.select(
            get_json_object(col("line"), "$.exchange").alias("exchange"),
            get_json_object(col("line"), "$.symbol").alias("symbol"),
            get_json_object(col("line"), "$.bid_price").cast("double").alias("bid"),
            get_json_object(col("line"), "$.ask_price").cast("double").alias("ask"),
            get_json_object(col("line"), "$.bid_qty").cast("double").alias("bid_q"),
            get_json_object(col("line"), "$.ask_qty").cast("double").alias("ask_q"),
            get_json_object(col("line"), "$.ts_ingest_ms").cast("long").alias("ts")
        ).filter(col("bid").isNotNull()).dropDuplicates(["exchange", "symbol", "ts"])

        binance = df_parsed.filter(col("exchange") == "binance").withColumn("t", (col("ts")/1000).cast("long")) \
            .select("symbol", "t", col("bid").alias("b_bid"), col("ask").alias("b_ask"), col("bid_q").alias("b_bid_q"), col("ask_q").alias("b_ask_q"))
        
        kraken = df_parsed.filter(col("exchange") == "kraken").withColumn("t", (col("ts")/1000).cast("long")) \
            .select("symbol", "t", col("bid").alias("k_bid"), col("ask").alias("k_ask"), col("bid_q").alias("k_bid_q"), col("ask_q").alias("k_ask_q"))

        joined = binance.join(kraken, ["symbol", "t"])

        # 3. Logic: Calculate Arbitrage AND define Buy/Sell Prices
        roi_b2k = (col("k_bid") - col("b_ask")) / col("b_ask")
        roi_k2b = (col("b_bid") - col("k_ask")) / col("k_ask")
        
        is_b2k = roi_b2k > roi_k2b
        
        df_out = joined.withColumn("roi_pct", when(is_b2k, roi_b2k).otherwise(roi_k2b) * 100) \
            .withColumn("trade_qty", when(is_b2k, least(col("b_ask_q"), col("k_bid_q"))).otherwise(least(col("k_ask_q"), col("b_bid_q")))) \
            .withColumn("direction", when(is_b2k, lit("Binance->Kraken")).otherwise(lit("Kraken->Binance"))) \
            .withColumn("readable_time", from_unixtime(col("t"))) \
            .withColumn("rowkey", concat(col("symbol"), lit("|"), (col("t")*1000).cast("string"))) \
            .withColumn("buy_price", when(is_b2k, col("b_ask")).otherwise(col("k_ask"))) \
            .withColumn("sell_price", when(is_b2k, col("k_bid")).otherwise(col("b_bid"))) \
            .withColumn("profit_usdt", col("trade_qty") * (col("sell_price") - col("buy_price"))) \
            .filter(col("roi_pct") > 0)

        if df_out.count() > 0:
            print(f">>> Writing to HBase: {Config.TABLE_BATCH_BBO}")
            df_out.foreachPartition(
                HBaseWriter.get_partition_writer(Config.TABLE_BATCH_BBO, BatchBBOJob._mapper)
            )

            # 4. Write to Hive (History) with new columns
            print(">>> Writing to Hive (BBO Results)...")
            df_hive = df_out.withColumn("processed_at", current_timestamp()) \
                            .withColumn("dt", to_date(col("readable_time")))
            
            # Added buy_price and sell_price here
            df_hive_clean = df_hive.select(
                "symbol", "direction", "buy_price", "sell_price", 
                "trade_qty", "profit_usdt", "roi_pct", 
                "readable_time", "processed_at", "dt"
            )
            
            df_hive_clean.write.mode("append").format("parquet").partitionBy("dt").saveAsTable("arbitrage_bbo_batch")
            print("SUCCESS - BBO results saved.")
        else:
            print("No BBO opportunities found.")

        self.stop()

if __name__ == "__main__":
    job = BatchBBOJob()
    job.run()