import happybase
from lib.config import Config

class HBaseWriter:
    
    @staticmethod
    def get_partition_writer(table_name, row_mapper_func):
        """
        Returns a closure (function) to be used in foreachPartition.
        This ensures the connection is opened on the worker nodes.
        
        Args:
            table_name: The HBase table to write to.
            row_mapper_func: A function taking a Spark Row and returning (RowKey, DataDict).
        """
        def process_partition(rows_iter):
            # Connection is created INSIDE the worker
            try:
                conn = happybase.Connection(
                    host=Config.HBASE_HOST,
                    port=Config.HBASE_PORT,
                    autoconnect=False
                )
                conn.open()
                table = conn.table(table_name)
                
                # Write in batches for performance
                with table.batch(batch_size=500) as bch:
                    for row in rows_iter:
                        try:
                            rk, data = row_mapper_func(row)
                            if rk and data:
                                bch.put(rk, data)
                        except Exception as e:
                            # Log malformed row but continue batch
                            continue
                            
                conn.close()
            except Exception as e:
                print(f"!!! HBase Connection Error: {e}")
                
        return process_partition