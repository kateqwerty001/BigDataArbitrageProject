import sys
import os
import shutil

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql.functions import col, from_json, to_timestamp, expr, from_unixtime
from lib.spark_base import SparkJobBase
from lib.config import Config
from lib.schemas import Schemas
from lib.calculator import ArbitrageCalculator
from lib.storage import HBaseWriter

class StreamingBBOJob(SparkJobBase):
    def __init__(self):
        super().__init__("StreamingBBO")

    @staticmethod
    def _mapper(row):
        """Maps Spark Row to HBase columns."""
        # RowKey: Symbol | Time
        rk = f"{row['symbol']}|{row['bin_time_ms']}".encode("utf-8")
        
        def to_bytes(val):
            return str(val).encode("utf-8")

        data = {
            b"time:val": to_bytes(row["readable_time"]),
            b"info:sym": to_bytes(row["symbol"]),
            b"info:dir": to_bytes(row["direction"]),
            b"signal:roi": to_bytes(round(row["roi_pct"], 4)),
            b"signal:profit_usdt": to_bytes(round(row["profit_usdt"], 2)),
            b"info:volume": to_bytes(row["exec_volume"])
        }
        return rk, data

    @staticmethod
    def _save_batch(df, epoch_id):
        df.persist()
        count = df.count()
        print(f"\n>>> [BATCH {epoch_id}]")
        if count > 0:
            print(f"\n>>> FOUND {count} BBO SIGNALS")
            
            # Console Preview
            df.select("readable_time", "symbol", "direction", "profit_usdt", "roi_pct") \
              .show(5, truncate=False)
            
            # Write to HBase
            try:
                df.foreachPartition(
                    HBaseWriter.get_partition_writer(Config.TABLE_STREAM_BBO, StreamingBBOJob._mapper)
                )
                print(f">>> Signals saved to HBase table: {Config.TABLE_STREAM_BBO}")
            except Exception as e:
                print(f"!!! HBase Write Error: {e}")
        
        df.unpersist()

    def run(self):
        self.spark.sparkContext.setLogLevel("ERROR")
        
        # Clean Start Logic
        checkpoint_dir = "/tmp/spark_checkpoint_bbo_project"
        if os.path.exists(checkpoint_dir):
            try:
                shutil.rmtree(checkpoint_dir)
                print(">>> Old checkpoints deleted. Starting FRESH.")
            except: pass

        # 1. Read
        raw = self.spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", Config.KAFKA_BOOTSTRAP) \
            .option("subscribe", Config.TOPIC_BBO) \
            .option("startingOffsets", "latest") \
            .load()

        # 2. Parse (Using Schema from lib)
        parsed = raw.selectExpr("CAST(value AS STRING) AS json_str") \
            .select(from_json(col("json_str"), Schemas.bbo_schema).alias("data")).select("data.*") \
            .withColumn("bid", col("bid_price").cast("double")) \
            .withColumn("ask", col("ask_price").cast("double")) \
            .withColumn("b_qty", col("bid_qty").cast("double")) \
            .withColumn("a_qty", col("ask_qty").cast("double")) \
            .withColumn("event_time", to_timestamp(col("ts_ingest_ms").cast("long")/1000)) \
            .withWatermark("event_time", "20 seconds")

        # 3. Split
        binance = parsed.filter(col("exchange") == "binance") \
            .select(col("symbol"), col("event_time").alias("bin_time"),
                    col("ts_ingest_ms").alias("bin_time_ms"),
                    col("bid").alias("bin_bid"), col("ask").alias("bin_ask"),
                    col("b_qty").alias("bin_bid_q"), col("a_qty").alias("bin_ask_q"))

        kraken = parsed.filter(col("exchange") == "kraken") \
            .select(col("symbol").alias("k_sym"), col("event_time").alias("krk_time"),
                    col("bid").alias("krk_bid"), col("ask").alias("krk_ask"),
                    col("b_qty").alias("krk_bid_q"), col("a_qty").alias("krk_ask_q"))

        # 4. Join
        interval = f"{Config.JOIN_WINDOW_SEC} seconds"
        joined = binance.join(kraken, expr(f"""
                symbol = k_sym AND
                bin_time >= krk_time - interval {interval} AND
                bin_time <= krk_time + interval {interval}
            """))

        # 5. Calculate (Using logic from Calculator lib)
        signals = ArbitrageCalculator.calculate_bbo_arbitrage(joined, Config.FEE_RATE) \
            .withColumn("readable_time", col("bin_time").cast("string"))

        # 6. Write
        print(f">>> Starting BBO Scanner... Target Table: {Config.TABLE_STREAM_BBO}")
        
        query = signals.writeStream \
            .foreachBatch(StreamingBBOJob._save_batch) \
            .trigger(processingTime=Config.STREAM_TRIGGER) \
            .option("checkpointLocation", f"file://{checkpoint_dir}") \
            .start()

        query.awaitTermination()

if __name__ == "__main__":
    job = StreamingBBOJob()
    job.run()