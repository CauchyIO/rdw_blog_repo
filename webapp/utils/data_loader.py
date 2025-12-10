"""Data loading utilities for the webapp."""

import pandas as pd
from typing import Optional

from databricks.connect import DatabricksSession


def _get_spark_session():
    """Get Spark session with serverless compute.

    In Databricks Apps, credentials are automatically available from the environment.
    """
    return DatabricksSession.builder.serverless(True).getOrCreate()


def load_car_data(
    limit: Optional[int] = None,
    table_name: str = "gold_catalog.rdw_etl.gekentekende_voertuigen_filtered"
) -> tuple[pd.DataFrame, list[str], list[str], list[str]]:
    """Load car data from Databricks gold table and extract unique filter values.

    Args:
        limit: Optional row limit for testing/development. None loads all data.
        table_name: Unity Catalog table path to load from.

    Returns:
        Tuple of (pandas_dataframe, brands_list, vehicle_types_list, colors_list)
    """
    try:
        print("Initializing Spark connection...")
        spark = _get_spark_session()

        print(f"Loading data from {table_name}...")
        spark_df = spark.table(table_name)
        spark_df = spark_df.filter(spark_df.voertuigsoort == "Personenauto")

        if limit:
            print(f"Limiting to {limit} rows...")
            spark_df = spark_df.limit(limit)

        # Convert to Pandas
        df_pandas = spark_df.toPandas()
        print(f"Loaded {len(df_pandas)} records into memory")

        # Get unique values for categorical filters
        brands = sorted([
            x for x in df_pandas["merk"].unique()
            if x and x != "Niet geregistreerd" and pd.notna(x)
        ])
        vehicle_types = sorted([
            x for x in df_pandas["voertuigsoort"].unique()
            if x and x != "Niet geregistreerd" and pd.notna(x)
        ])
        colors = sorted([
            x for x in df_pandas["eerste_kleur"].unique()
            if x and x != "Niet geregistreerd" and pd.notna(x)
        ])

        print(f"Loaded metadata: {len(brands)} brands, {len(vehicle_types)} types, {len(colors)} colors")

        return df_pandas, brands, vehicle_types, colors

    except Exception as e:
        import traceback
        print(f"Could not load data: {e}")
        traceback.print_exc()
        return pd.DataFrame(), [], [], []


def get_spark():
    """Get or create Spark session for direct queries."""
    return _get_spark_session()