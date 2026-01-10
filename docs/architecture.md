# BigDataArbitrageProject — Lambda Architecture (Frozen v1)

## Goal
Collect market data for **ETHUSDT** from **Binance** and **Kraken**, store raw history, and compute arbitrage-related metrics in near-real time and in batch mode.

---

## Lambda Architecture Overview

We implement a classic Lambda Architecture with 3 layers:

### 1) Batch Layer (Master Dataset)
**Purpose:** Store immutable raw data for long-term historical analysis and reprocessing.

**Tech:**
- HDFS (raw archive and processed files)
- Hive (external tables for query + partitioning)

**Data stored:**
- Raw JSON events from both exchanges (orderbook, bbo, trades)
- Normalized events (optional) and derived batch outputs (Parquet)

---

### 2) Speed Layer (Near-real-time Processing)
**Purpose:** Compute near-real-time arbitrage signals and metrics from streaming data.

**Tech:**
- Kafka (message broker, buffering, decoupling ingestion from processing)
- Spark Structured Streaming (micro-batch streaming jobs)

**Processing examples:**
- Compute spread:
  - spread = best_ask_exchangeA - best_bid_exchangeB
- Detect opportunities:
  - if spread > threshold and liquidity sufficient
- Compute rolling metrics:
  - moving average spread, volatility proxy, liquidity depth changes

Outputs from speed layer go to:
- Serving layer (HBase) for fast reads / dashboards
- HDFS (optional) for streaming archive

---

### 3) Serving Layer (Low Latency Views)
**Purpose:** Provide fast read access to precomputed views/aggregations.

**Tech:**
- HBase

**Examples of served views:**
- Latest BBO per exchange
- Latest spread per pair
- Aggregated spread statistics per minute/hour
- Liquidity summary per interval

---

## Data Flow (High Level)

1. **NiFi** pulls data from REST APIs:
   - Binance: depth, bookTicker, trades
   - Kraken: Depth, Ticker, Trades

2. NiFi normalizes records (canonical JSON schema) and publishes to Kafka topics:
- `crypto.trades.normalized`
- `crypto.bbo.normalized`
- `crypto.orderbook.normalized`

3. **Spark Structured Streaming** consumes Kafka topics and:
- computes near-real-time metrics
- writes serving tables to HBase
- optionally archives to HDFS

4. **Spark batch jobs** read historical data from Hive/HDFS and:
- recompute metrics over longer windows
- create batch views (daily/hourly stats)
- write final batch views to HBase

---

## Storage Layout Proposal (HDFS)

Raw:
- `/data/raw/binance/trades/dt=YYYY-MM-DD/`
- `/data/raw/kraken/trades/dt=YYYY-MM-DD/`
- (same for bbo and orderbook)

Normalized:
- `/data/normalized/trades/dt=YYYY-MM-DD/`
- `/data/normalized/bbo/dt=YYYY-MM-DD/`
- `/data/normalized/orderbook/dt=YYYY-MM-DD/`

Batch results:
- `/data/batch/spread_stats/dt=YYYY-MM-DD/`
- `/data/batch/liquidity_stats/dt=YYYY-MM-DD/`

---

## HBase Tables (Serving Layer Proposal)

- `bbo_latest`
  - rowkey: `ETHUSDT#binance` / `ETHUSDT#kraken`
  - columns: bid_price, ask_price, bid_qty, ask_qty, ts_event_ms

- `spread_latest`
  - rowkey: `ETHUSDT`
  - columns: spread_binance_ask_minus_kraken_bid, spread_kraken_ask_minus_binance_bid, ts_event_ms

- `spread_stats_1m`
  - rowkey: `ETHUSDT#YYYYMMDDHHMM`
  - columns: avg_spread, max_spread, min_spread, count

(Exact schema can be adjusted after first Spark job.)

---

## Quality & Testing Hooks (for grading)
- Kafka topics are schema-stable (based on `docs/data_schema.md`)
- NiFi flows include failure handling (failure relationship to log / dead-letter topic)
- Spark jobs will have:
  - unit tests for parsing/normalization logic
  - integration tests using sample JSON fixtures from `examples/`

