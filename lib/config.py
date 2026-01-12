class Config:
    # --- Spark Settings ---
    APP_NAME_PREFIX = "CryptoArb_"
    LOG_LEVEL = "WARN"

    # --- Kafka Topics ---
    KAFKA_BOOTSTRAP = "localhost:9092"
    TOPIC_DEPTH = "crypto.depth"
    TOPIC_BBO = "crypto.bbo"
    TOPIC_TRADES = "crypto.trades"

    # --- HBase Settings ---
    HBASE_HOST = "localhost"
    HBASE_PORT = 9090
    
    # Table Names (HBase)
    TABLE_REALTIME_DEPTH = "arbitrage_depth_realtime"
    TABLE_BATCH_DEPTH = "arbitrage_depth_results"
    TABLE_BATCH_TRADES_CONFIRM = "arbitrage_trade_confirmed"
    TABLE_BATCH_BBO = "arbitrage_results" 
    TABLE_STREAM_BBO = "arbitrage_bbo_signals"

    # --- Hive Tables (Inputs for Batch) ---
    HIVE_TABLE_DEPTH = "raw_depth_json"
    HIVE_TABLE_TRADES = "raw_trades_json"
    HIVE_TABLE_BBO = "raw_bbo_json"

    # --- Business Logic Configuration ---
    TRADE_QUOTE_USDT = 500.0
    FEE_RATE = 0.000  # Set to 0.001 for 0.1% fee
    JOIN_WINDOW_SEC = 2
    STREAM_TRIGGER = "0 seconds"