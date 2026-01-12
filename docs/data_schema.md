# Data Contracts — BigDataArbitrageProject (Frozen v1)

This document defines the canonical schemas for all market data events.
All raw data from Binance and Kraken MUST be normalized to these schemas
before being sent to Kafka or stored in HDFS/Hive.

---

## 1. Frozen Project Scope

- Asset pair: ETHUSDT
- Canonical symbol: ETH/USDT
- Exchanges:
  - binance
  - kraken
- Event types:
  - trade
  - bbo (best bid / best ask)
  - depth

All timestamps are stored in UTC milliseconds.

---

## 2. Common Fields (All Events)

| Field | Type | Description |
|-----|-----|------------|
| event_type | string | trade \| bbo \| depth |
| exchange | string | binance \| kraken |
| symbol | string | ETH/USDT |
| source_symbol | string | ETHUSDT |
| ts_event_ms | long | Event timestamp |
| ts_ingest_ms | long | Ingestion timestamp |
| ingest_id | string | UUID generated in NiFi |

---

## 3. Trade Event Schema

| Field | Type | Description |
|-----|-----|------------|
| trade_id | string | Source trade id |
| price | double | Trade price |
| qty | double | Trade quantity |
| side | string | buy or sell |

Notes:
- Binance: isBuyerMaker=true ⇒ side=sell
- Kraken: side is provided directly ("b" or "s")

---

## 4. BBO Schema

| Field | Type |
|-----|-----|
| bid_price | double |
| bid_qty | double |
| ask_price | double |
| ask_qty | double |

---

## 5. Depth Snapshot Schema

| Field | Type |
|-----|-----|
| depth | int |
| bids | array |
| asks | array |

---

## 6. Kafka Topics

- crypto.trades
- crypto.bbo
- crypto.depth
