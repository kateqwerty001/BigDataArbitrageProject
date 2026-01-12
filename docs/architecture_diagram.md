```mermaid
flowchart LR
  subgraph Sources
    B[Binance REST API<br/>Depth / BookTicker / Trades]
    K[Kraken REST API<br/>Depth / Ticker / Trades]
  end

  subgraph Ingestion
    N[Apache NiFi<br/>InvokeHTTP + Normalize]
  end

  subgraph Speed["Speed Layer"]
    KF[Kafka Topics<br/> trades / bbo / depth]
    SS[Spark Structured Streaming<br/>BBO spread + ROI]
  end

  subgraph Batch["Batch Layer"]
    HDFS[HDFS Raw + Normalized]
    Hive[Hive External Tables]
    SB[Spark Batch Jobs<br/>hour / day aggregations]
  end

  subgraph Serving["Serving Layer"]
    HB[HBase Views<br/>latest + aggregated]
  end

  B --> N
  K --> N
  N --> KF
  KF --> SS
  SS --> HB
  N --> HDFS
  HDFS --> Hive
  Hive --> SB
  SB --> HB
