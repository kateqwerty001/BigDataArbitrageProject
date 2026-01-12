# BigDataArbitrageProject

Big data pipeline for ETH/USDT arbitrage analytics across Binance and Kraken using a Lambda Architecture (batch + speed + serving).

## Project Goal
Collect market data for **ETHUSDT** from **Binance** and **Kraken**, normalize it to a canonical schema, and compute arbitrage metrics:
- Near-real time BBO spread/ROI (speed layer)
- Batch depth and trade-confirmed arbitrage (batch layer)

Canonical symbol: **ETH/USDT** with `source_symbol` **ETHUSDT**.

## High-Level Goals
- Deliver fast arbitrage signals from streaming BBO data for realtime insight
- Maintain a durable historical dataset for backtesting and reprocessing
- Compute batch BBO arbitrage metrics (ROI and profit), depth VWAP arbitrage metrics (ROI and profit), and trade-confirmed summaries (buy/sell counts, profit)

## Architecture (Lambda)
- **Ingestion:** NiFi pulls REST data (trades, BBO, depth), normalizes records, and publishes to Kafka.
- **Batch layer:** Raw JSON archived to HDFS and queried via Hive external tables; Spark batch jobs compute depth and trade-confirmed analytics.
- **Speed layer:** Spark Structured Streaming consumes BBO topic and writes realtime signals.
- **Serving layer:** HBase tables hold realtime and batch outputs for fast reads.

See `docs/architecture.md` and `docs/architecture_diagram.md` for the full flow.

## Data Contracts
Schemas and canonical event formats live in `docs/data_schema.md`. All ingestion must normalize to:
- `trade`
- `bbo`
- `depth`

Kafka topics:
- `crypto.trades`
- `crypto.bbo`
- `crypto.depth`

## Repo Layout
- `docs/` Architecture, schema, and diagram docs
- `nifi/` NiFi flow definitions (XML)
- `jobs/` Spark batch + streaming jobs
- `lib/` Shared Spark logic, schemas, config, storage
- `hive_sql/` Hive external table definitions
- `tests/` Unit tests for core logic

## Key Jobs
Streaming:
- `jobs/streaming_bbo_job.py` -> HBase `arbitrage_bbo_signals`

Batch:
- `jobs/batch_bbo_job.py` -> HBase `arbitrage_results`
- `jobs/batch_depth_job.py` -> HBase `arbitrage_depth_results` + Hive `arbitrage_depth_batch`
- `jobs/batch_trades_confirm_job.py` -> HBase `arbitrage_trade_confirmed` + Hive `arbitrage_trades_confirmed_batch`

Serving:
- `jobs/serving_layer.py` (reads from HBase and prints recent signals)

## Storage & Tables
HDFS layout (raw JSON):
- `/user/vagrant/bigdata_arbitrage/raw/trades/dt=YYYY-MM-DD/`
- `/user/vagrant/bigdata_arbitrage/raw/bbo/dt=YYYY-MM-DD/`
- `/user/vagrant/bigdata_arbitrage/raw/depth/dt=YYYY-MM-DD/`

Hive tables:
- `raw_trades_json`
- `raw_bbo_json`
- `raw_depth_json`
- `arbitrage_depth_batch`
- `arbitrage_trades_confirmed_batch`

HBase tables:
- `arbitrage_bbo_signals`
- `arbitrage_results`
- `arbitrage_depth_results`
- `arbitrage_trade_confirmed`

## Configuration
Runtime settings live in `lib/config.py`:
- Kafka bootstrap and topic names
- HBase host/port and table names
- Arbitrage parameters (fees, join window, trade quote size)

## Running (Typical Flow)
1) Deploy NiFi flows from `nifi/` to ingest and normalize data.
2) Create Hive external tables from `hive_sql/raw_external_tables.sql`.
3) Run streaming BBO job:
   - `python jobs/streaming_bbo_job.py`
4) Run batch jobs for a target date (YYYY-MM-DD):
   - `python jobs/batch_bbo_job.py 2024-01-01`
   - `python jobs/batch_depth_job.py 2024-01-01`
   - `python jobs/batch_trades_confirm_job.py 2024-01-01`
5) View results with:
   - `python jobs/serving_layer.py`

## Tests
Run unit tests:
```bash
pytest
```

## Notes
- Assumes a local Kafka/HBase/HDFS/Hive environment.
- Spark jobs expect normalized JSON (per `docs/data_schema.md`).
