import sys
import os
import happybase
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib.config import Config

class ServingLayer:
    def __init__(self):
        self.host = Config.HBASE_HOST
        self.port = Config.HBASE_PORT
        self.conn = None

    def connect(self):
        try:
            self.conn = happybase.Connection(host=self.host, port=self.port, autoconnect=False)
            self.conn.open()
        except Exception as e:
            print(f"!!! Error connecting to HBase: {e}")
            sys.exit(1)

    def close(self):
        if self.conn:
            self.conn.close()

    def get_realtime_opportunities(self, limit=5):
        print(f"\n--- [SPEED LAYER] LIVE BBO SIGNALS ({Config.TABLE_STREAM_BBO}) ---")
        try:
            table = self.conn.table(Config.TABLE_STREAM_BBO)
            count = 0
            
            for key, data in table.scan(reverse=True, limit=limit):
                try:
                    ts_val = data.get(b'time:val', b'N/A').decode('utf-8')
                    symbol = data.get(b'info:sym', b'Unknown').decode('utf-8')
                    direction = data.get(b'signal:dir', b'--').decode('utf-8')
                    profit = data.get(b'signal:profit_usdt', b'0.0').decode('utf-8')
                    
                    print(f"[{ts_val}] {symbol} | {direction} | Profit: ${profit}")
                    count += 1
                except Exception as e:
                    print(f"Error parsing row: {e}")

            if count == 0: 
                print("No live signals found (Streaming job running?).")
                
        except Exception as e:
            print(f"Connection error or Table not found: {e}")

    def get_confirmed_trades(self, limit=5):
        print(f"\n--- [SERVING LAYER] CONFIRMED TRADES ({Config.TABLE_BATCH_TRADES_CONFIRM}) ---")
        try:
            table = self.conn.table(Config.TABLE_BATCH_TRADES_CONFIRM)
            count = 0
            for key, data in table.scan(limit=limit):
                symbol = data[b'info:sym'].decode()
                buy_cnt = data[b'trade_info:buy_count'].decode()
                sell_cnt = data[b'trade_info:sell_count'].decode()
                profit = data[b'profit_metrics:profit_depth_500_usdt'].decode()
                print(f"CONFIRMED: {symbol} | Executed: {buy_cnt} Buys, {sell_cnt} Sells | Proift: ${profit}")
                count += 1
            if count == 0: print("No confirmed trades yet.")
        except: print("Table not found.")

if __name__ == "__main__":
    app = ServingLayer()
    app.connect()
    app.get_realtime_opportunities()
    app.get_confirmed_trades()
    app.close()