import sys
import os
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql.functions import (
    col, get_json_object, from_json, lit, concat, from_unixtime, to_date,
    current_timestamp, udf, when, count as f_count, sum as f_sum
)
from lib.spark_base import SparkJobBase
from lib.config import Config
from lib.schemas import Schemas
from lib.calculator import ArbitrageCalculator
from lib.storage import HBaseWriter

class BatchTradesConfirmJob(SparkJobBase):
    def __init__(self):
        super().__init__("BatchTradesConfirm")

    @staticmethod
    def _mapper(r):
        rk = r["rowkey"].encode("utf-8")
        def bstr(x): return str(x).encode("utf-8")
        
        data = {
            b"time:val": bstr(r["readable_time"]),
            b"info:sym": bstr(r["symbol"]),
            b"info:dir": bstr(r["direction"]),
            b"confirm:both_sides_traded": bstr(r["both_sides_traded"]),
            b"profit_metrics:profit_depth_500_usdt": bstr(r["profit_depth_500_usdt"]),
            b"trade_info:buy_count": bstr(r["buy_trade_count"]),
            b"trade_info:sell_count": bstr(r["sell_trade_count"]),
        }
        return rk, data

    def run(self):
        target_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
        print(f">>> Target Date: {target_date}")
        self.spark.sql("USE default")

        # --- 1. PREPARE DEPTH ARBITRAGE (Depth Logic) ---
        try:
            df_depth_raw = self.spark.sql(f"SELECT line FROM {Config.HIVE_TABLE_DEPTH} WHERE dt='{target_date}'")
        except:
            print("!!! Hive table for Depth not found.")
            return

        if df_depth_raw.rdd.isEmpty(): 
            print("!!! No Depth data found.")
            return

        # Fixed timestamp aliasing
        df_depth = df_depth_raw.select(
            get_json_object(col("line"), "$.exchange").alias("exchange"),
            get_json_object(col("line"), "$.symbol").alias("symbol"),
            get_json_object(col("line"), "$.ts_ingest_ms").cast("long").alias("ts"),
            from_json(get_json_object(col("line"), "$.asks"), Schemas.level_schema).alias("asks"),
            from_json(get_json_object(col("line"), "$.bids"), Schemas.level_schema).alias("bids"),
        ).filter(col("exchange").isNotNull()).withColumn("time_sec", (col("ts") / 1000).cast("long")) \
         .dropDuplicates(["exchange", "symbol", "time_sec"])

        b = df_depth.filter(col("exchange") == "binance").alias("b")
        k = df_depth.filter(col("exchange") == "kraken").alias("k")
        joined = b.join(k, (col("b.symbol") == col("k.symbol")) & (col("b.time_sec") == col("k.time_sec")))

        buy_udf = udf(ArbitrageCalculator.buy_with_quote, Schemas.buy_ret_schema)
        sell_udf = udf(ArbitrageCalculator.sell_base_for_quote, Schemas.sell_ret_schema)

        # Calculate Buys/Sells
        df_calc = joined.withColumn("b_buy", buy_udf(col("b.asks"), lit(Config.TRADE_QUOTE_USDT))) \
                        .withColumn("k_buy", buy_udf(col("k.asks"), lit(Config.TRADE_QUOTE_USDT)))

        df_calc = df_calc.withColumn("k_sell", sell_udf(col("k.bids"), col("b_buy.base_qty"))) \
                         .withColumn("b_sell", sell_udf(col("b.bids"), col("k_buy.base_qty")))

        # Logic
        p_b2k = (col("k_sell.revenue") * (1 - Config.FEE_RATE)) - (col("b_buy.cost_used") * (1 + Config.FEE_RATE))
        p_k2b = (col("b_sell.revenue") * (1 - Config.FEE_RATE)) - (col("k_buy.cost_used") * (1 + Config.FEE_RATE))
        roi_b2k = p_b2k / col("b_buy.cost_used")
        roi_k2b = p_k2b / col("k_buy.cost_used")
        is_b2k = p_b2k > p_k2b

        # Prepare Opportunities DataFrame (Keep necessary columns for final schema)
        df_opps = df_calc.select(
            col("b.symbol").alias("symbol"), 
            col("b.time_sec").alias("time_sec"),
            when(is_b2k, lit("binance")).otherwise(lit("kraken")).alias("buy_exchange"),
            when(is_b2k, lit("kraken")).otherwise(lit("binance")).alias("sell_exchange"),
            when(is_b2k, lit("Binance -> Kraken")).otherwise(lit("Kraken -> Binance")).alias("direction"),
            when(is_b2k, p_b2k).otherwise(p_k2b).alias("profit_depth_500_usdt"),
            
            # Needed for Schema Matching
            when(is_b2k, col("b_buy.cost_used")).otherwise(col("k_buy.cost_used")).alias("cost_used_usdt"),
            when(is_b2k, col("b_buy.base_qty")).otherwise(col("k_buy.base_qty")).alias("base_qty"),
            when(is_b2k, col("b_buy.vwap_buy")).otherwise(col("k_buy.vwap_buy")).alias("depth_vwap_buy"),
            when(is_b2k, col("k_sell.vwap_sell")).otherwise(col("b_sell.vwap_sell")).alias("depth_vwap_sell"),
            when(is_b2k, col("b_buy.levels_buy")).otherwise(col("k_buy.levels_buy")).alias("levels_buy"),
            when(is_b2k, col("k_sell.levels_sell")).otherwise(col("b_sell.levels_sell")).alias("levels_sell"),
            (when(is_b2k, roi_b2k).otherwise(roi_k2b) * 100.0).alias("roi_depth_500_pct")
        ).filter(col("profit_depth_500_usdt") > 0)

        # --- 2. PREPARE TRADES ---
        try:
            df_trades_raw = self.spark.sql(f"SELECT line FROM {Config.HIVE_TABLE_TRADES} WHERE dt='{target_date}'")
        except:
             print("!!! Hive table for Trades not found.")
             return
             
        if df_trades_raw.rdd.isEmpty(): 
            print("!!! No Trades data found.")
            return

        df_trades = df_trades_raw.select(
            from_json(col("line"), Schemas.trade_schema).alias("t")
        ).select("t.*") \
        .withColumn("price", col("price").cast("double")) \
        .withColumn("qty", col("qty").cast("double")) \
        .withColumn("ts", col("ts_ingest_ms").cast("long")) \
        .withColumn("time_sec", (col("ts")/1000).cast("long"))

        # Aggregate trades per second/exchange/side (Calculated Weighted Average Price if possible, else 0)
        trades_agg = df_trades.groupBy("symbol", "exchange", "time_sec", "side").agg(
            f_count(lit(1)).alias("count"),
            f_sum(col("qty")).alias("qty"),
            (f_sum(col("qty") * col("price")) / f_sum(col("qty"))).alias("vwap")
        )

        # Separate into buy tables and sell tables for joining
        # We rename columns to avoid ambiguity during join
        buy_tr = trades_agg.filter(col("side") == "buy").select(
            col("symbol").alias("buy_sym"), 
            col("exchange").alias("buy_ex"), 
            col("time_sec").alias("buy_ts"), 
            col("count").alias("buy_trade_count"),
            col("qty").alias("buy_trade_qty"),
            col("vwap").alias("buy_trade_vwap")
        )
        
        sell_tr = trades_agg.filter(col("side") == "sell").select(
            col("symbol").alias("sell_sym"), 
            col("exchange").alias("sell_ex"), 
            col("time_sec").alias("sell_ts"), 
            col("count").alias("sell_trade_count"),
            col("qty").alias("sell_trade_qty"),
            col("vwap").alias("sell_trade_vwap")
        )

        # --- 3. CONFIRMATION JOIN (10 SEC WINDOW) ---

        # 1. Join with BUY TRADES
        cond_buy = (
            (df_opps.symbol == buy_tr.buy_sym) & 
            (df_opps.buy_exchange == buy_tr.buy_ex) & 
            (buy_tr.buy_ts >= df_opps.time_sec) & 
            (buy_tr.buy_ts <= df_opps.time_sec + 20)
        )
        confirmed_step1 = df_opps.join(buy_tr, cond_buy, "left")
        
        # 2. Join result with SELL TRADES
        cond_sell = (
            (confirmed_step1.symbol == sell_tr.sell_sym) & 
            (confirmed_step1.sell_exchange == sell_tr.sell_ex) & 
            (sell_tr.sell_ts >= confirmed_step1.time_sec) & 
            (sell_tr.sell_ts <= confirmed_step1.time_sec + 20)
        )
        confirmed_final = confirmed_step1.join(sell_tr, cond_sell, "left")

        # Select and cleanup
        df_out = confirmed_final \
            .fillna({
                "buy_trade_count": 0, "sell_trade_count": 0,
                "buy_trade_qty": 0.0, "sell_trade_qty": 0.0,
                "buy_trade_vwap": 0.0, "sell_trade_vwap": 0.0
            }) \
            .withColumn("both_sides_traded", (col("buy_trade_count") > 0) & (col("sell_trade_count") > 0)) \
            .filter(col("both_sides_traded") == lit(True)) \
            .withColumn("readable_time", from_unixtime(col("time_sec"))) \
            .withColumn("rowkey", concat(col("symbol"), lit("|"), (col("time_sec") * 1000).cast("string")))

        # --- 4. WRITE ---
        if df_out.count() > 0:
            print(f">>> Writing confirmed trades to: {Config.TABLE_BATCH_TRADES_CONFIRM}")
            df_out.foreachPartition(
                HBaseWriter.get_partition_writer(Config.TABLE_BATCH_TRADES_CONFIRM, BatchTradesConfirmJob._mapper)
            )

            print(">>> Writing to Hive (Confirmed Trades)...")
            
            df_hive = df_out.withColumn("processed_at", current_timestamp()) \
                            .withColumn("dt", to_date(col("readable_time")))
            
            df_hive_clean = df_hive.select(
                "rowkey", "readable_time", "symbol", "direction", "buy_exchange", "sell_exchange",
                "cost_used_usdt", "base_qty", "depth_vwap_buy", "depth_vwap_sell", "levels_buy", "levels_sell",
                "roi_depth_500_pct", "profit_depth_500_usdt",
                "buy_trade_count", "buy_trade_qty", "buy_trade_vwap",
                "sell_trade_count", "sell_trade_qty", "sell_trade_vwap",
                "both_sides_traded", "processed_at", "dt"
            )

            df_hive_clean.write.mode("append").format("parquet").partitionBy("dt").saveAsTable("arbitrage_trades_confirmed_batch")
            print("SUCCESS - SAVED TO HBASE abd HIVE")
        else:
            print("No confirmed trades found.")

        self.stop()

if __name__ == "__main__":
    job = BatchTradesConfirmJob()
    job.run()