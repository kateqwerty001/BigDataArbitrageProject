import pytest
from pyspark.sql import SparkSession
import sys
import os

# Ensure the root project directory is in the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

@pytest.fixture(scope="session")
def spark():
    """
    Creates a local SparkSession for testing purposes.
    """
    spark_session = (SparkSession.builder
        .master("local[1]")
        .appName("ArbitrageUnitTests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate())
    
    yield spark_session
    
    spark_session.stop()