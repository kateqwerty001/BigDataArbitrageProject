from pyspark.sql import SparkSession
from lib.config import Config

class SparkJobBase:
    """Base class to initialize Spark Session with Hive support."""
    
    def __init__(self, job_name):
        self.spark = SparkSession.builder \
            .appName(f"{Config.APP_NAME_PREFIX}{job_name}") \
            .enableHiveSupport() \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel(Config.LOG_LEVEL)

    def stop(self):
        self.spark.stop()