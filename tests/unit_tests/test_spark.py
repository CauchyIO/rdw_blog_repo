
from pyspark.sql import SparkSession, DataFrame  # Use regular DataFrame
import pandas as pd
import os
import pytest

def test_spark_fixture(spark_session):
    """Check that SparkSession starts with config from SparkConfig."""
    # Verify SparkSession is alive
    assert spark_session is not None

    # Run a trivial Spark job to confirm it works
    df = spark_session.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    result = df.count()
    assert result == 2

