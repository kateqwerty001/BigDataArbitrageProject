-- ============================================================
-- BigData Arbitrage: Raw Kafka -> HDFS -> Hive external tables
-- Data layout:
--   /user/vagrant/bigdata_arbitrage/raw/<dataset>/dt=YYYY-MM-DD/<files>
-- Each file contains 1 JSON message per file (or 1 JSON per line).
-- ============================================================

-- (Optional) choose DB
-- CREATE DATABASE IF NOT EXISTS bigdata_arbitrage;
-- USE bigdata_arbitrage;

-- ----------------------------
-- TRADES
-- ----------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS raw_trades_json (
  line STRING
)
PARTITIONED BY (dt STRING)
STORED AS TEXTFILE
LOCATION '/user/vagrant/bigdata_arbitrage/raw/trades';

-- ----------------------------
-- BBO
-- ----------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS raw_bbo_json (
  line STRING
)
PARTITIONED BY (dt STRING)
STORED AS TEXTFILE
LOCATION '/user/vagrant/bigdata_arbitrage/raw/bbo';

-- ----------------------------
-- DEPTH
-- ----------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS raw_depth_json (
  line STRING
)
PARTITIONED BY (dt STRING)
STORED AS TEXTFILE
LOCATION '/user/vagrant/bigdata_arbitrage/raw/depth';

-- ============================================================
-- Partition discovery
-- If your data folders already exist in HDFS, this will load them
-- into the metastore.
-- ============================================================

MSCK REPAIR TABLE raw_trades_json;
MSCK REPAIR TABLE raw_bbo_json;
MSCK REPAIR TABLE raw_depth_json;

-- Quick verification
SHOW PARTITIONS raw_trades_json;
SHOW PARTITIONS raw_bbo_json;
SHOW PARTITIONS raw_depth_json;

SELECT * FROM raw_trades_json LIMIT 5;
SELECT * FROM raw_bbo_json LIMIT 5;
SELECT * FROM raw_depth_json LIMIT 5;

-- ============================================================
-- If there is new data dt=...
-- MSCK REPAIR TABLE raw_trades_json;
-- MSCK REPAIR TABLE raw_bbo_json;
-- MSCK REPAIR TABLE raw_depth_json;
-- ============================================================
