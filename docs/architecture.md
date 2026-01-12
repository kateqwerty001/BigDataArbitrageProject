# BigDataArbitrageProject — Lambda Architecture (Frozen v1)

## Goal
Collect market data for **ETHUSDT** from **Binance** and **Kraken**, store raw history, and compute arbitrage metrics in near-real time (BBO) and in batch mode (BBO + depth).
Canonical symbol is **ETH/USDT** with `source_symbol` **ETHUSDT**.

---

## Lambda Architecture Overview

We implement a classic Lambda Architecture with 3 layers:

### 1) Batch Layer (Master Dataset)
**Purpose:** Store immutable raw data for long-term historical analysis and reprocessing.

**Tech:**
- HDFS (raw archive)
- Hive (external tables for query + partitioning)

**Data stored:**
- Raw JSON events (already normalized to the canonical schema in `docs/data_schema.md`)
- Derived batch outputs (Parquet tables)

---

### 2) Speed Layer (Near-real-time Processing)
**Purpose:** Compute near-real-time arbitrage signals and metrics from streaming data.

**Tech:**
- Kafka (message broker, buffering, decoupling ingestion from processing)
- Spark Structured Streaming (micro-batch streaming jobs)

**Processing examples:**
- Compute spread/ROI between exchanges from BBO snapshots

Outputs from speed layer go to:
- HBase (serving layer tables for fast reads / dashboards)

---

### 3) Serving Layer (Low Latency Views)
**Purpose:** Provide fast read access to precomputed views/aggregations.

**Tech:**
- HBase

**Examples of served views:**
- BBO arbitrage signals (real-time)
- Batch arbitrage results (BBO and depth)
- Trade-confirmed depth opportunities

---

## Data Flow (High Level)

1. **NiFi** pulls data from REST APIs:
   - Binance: depth (order book snapshot), bookTicker (BBO), trades
   - Kraken: Depth, Ticker (BBO), Trades

2. NiFi normalizes records to the canonical schema and publishes to Kafka topics:
- `crypto.trades`
- `crypto.bbo`
- `crypto.depth`
- `crypto.deadletter` (failed records)

3. **NiFi Kafka-to-HDFS** flow archives raw JSON to HDFS and exposes Hive tables:
- HDFS paths under `/user/vagrant/bigdata_arbitrage/raw/...`
- Hive external tables: `raw_trades_json`, `raw_bbo_json`, `raw_depth_json`

4. **Spark Structured Streaming** consumes Kafka topics and writes to HBase:
- `jobs/streaming_bbo_job.py` -> `arbitrage_bbo_signals`

5. **Spark batch jobs** read from Hive/HDFS and write batch outputs:
- `jobs/batch_bbo_job.py` -> `arbitrage_results` (HBase)
- `jobs/batch_depth_job.py` -> `arbitrage_depth_results` (HBase) + `arbitrage_depth_batch` (Hive)
- `jobs/batch_trades_confirm_job.py` -> `arbitrage_trade_confirmed` (HBase) + `arbitrage_trades_confirmed_batch` (Hive)

---

## Storage Layout (HDFS)

Raw (normalized JSON):
- `/user/vagrant/bigdata_arbitrage/raw/trades/dt=YYYY-MM-DD/`
- `/user/vagrant/bigdata_arbitrage/raw/bbo/dt=YYYY-MM-DD/`
- `/user/vagrant/bigdata_arbitrage/raw/depth/dt=YYYY-MM-DD/`

Batch outputs (Parquet via Hive):
- `arbitrage_depth_batch` (partitioned by `dt`)
- `arbitrage_trades_confirmed_batch` (partitioned by `dt`)

---

## HBase Tables (Serving Layer)

Real-time:
- `arbitrage_bbo_signals` (rowkey: `symbol|ts_ms`)

Batch:
- `arbitrage_results` (BBO batch; rowkey: `symbol|ts_ms`)
- `arbitrage_depth_results` (depth batch; rowkey: `symbol|ts_ms`)
- `arbitrage_trade_confirmed` (depth+trades confirmation; rowkey: `symbol|ts_ms`)

---

## Quality & Testing Hooks (for grading)
- Kafka topics are schema-stable (based on `docs/data_schema.md`)
- NiFi flows include failure handling (dead-letter topic)
- Spark jobs have unit coverage in `tests/`
