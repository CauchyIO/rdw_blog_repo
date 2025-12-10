import pytest
from pyspark.sql import SparkSession
from pydantic_settings import BaseSettings


class SparkConfig(BaseSettings):
    """Configuration class for application settings.

    This class loads settings from environment variables or a specified `.env` file
    and provides validation for the defined attributes.
    """

    master: str = "local[1]"
    app_name: str = "local_test"
    spark_executor_cores: str = "1"
    spark_executor_instances: str = "1"
    spark_sql_shuffle_partitions: str = "1"
    spark_driver_bindAddress: str = "127.0.0.1"


# Load configuration from environment variables
spark_config = SparkConfig()


@pytest.fixture(scope="function")
def spark_session() -> SparkSession:
    """Create and return a SparkSession for testing.

    This fixture creates a SparkSession with the specified configuration and returns it for use in tests.
    """
    from rdw import PROJECT_DIR
    import src.rdw.conf as rdw_conf
    
    # Override catalog config for local testing
    rdw_conf.bronze_catalog = "spark_catalog"
    
    catalog_dir = PROJECT_DIR / "tests" / "catalog"
    catalog_dir.mkdir(parents=True, exist_ok=True)
    
    spark = (
        SparkSession.builder.master(spark_config.master)
        .appName(spark_config.app_name)
        .config("spark.executor.cores", spark_config.spark_executor_cores)
        .config("spark.executor.instances", spark_config.spark_executor_instances)
        .config("spark.sql.shuffle.partitions", spark_config.spark_sql_shuffle_partitions)
        .config("spark.driver.bindAddress", spark_config.spark_driver_bindAddress)
        .getOrCreate()
    )

    yield spark
    spark.stop()