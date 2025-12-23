import re
import pyspark.sql.functions as F
import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DateType
from rdw.utils.models import BaseTable
import numpy as np
import os

def filter_first_col_length(df, length = 6):
    first_col = df.columns[0]
    return df.filter(F.length(first_col) == length)

def to_snake_case(col_name: str) -> str:
    c = col_name.lower()
    c = re.sub(r'[^a-z0-9]+', '_', c)
    c = re.sub(r'_+', '_', c)
    c = c.strip('_')
    return c

def convert_illegal_column_characters(df: DataFrame):
    for col in df.columns:
        df = df.withColumnRenamed(col, to_snake_case(col))
    return df


def convert_columns_from_definition(df: DataFrame, base_table: BaseTable) -> DataFrame:
    """
    Convert DataFrame columns based on BaseTable definition:
    - Renames input_col -> output_col
    - Casts to output_data_type
    - Special handling for DateType (assumes yyyyMMdd int/string format)
    """
    for column in base_table.columns:
        if column.input_col in df.columns:
            # Rename if input_col != output_col
            if column.input_col != column.output_col:
                df = df.withColumnRenamed(column.input_col, column.output_col)

            # Special case: DateType
            if isinstance(column.output_data_type, DateType):
                df = df.withColumn(
                    column.output_col,
                    F.to_date(F.col(column.output_col).cast("string"), "yyyyMMdd")
                )
            else:
                # Normal cast
                df = df.withColumn(
                    column.output_col,
                    F.col(column.output_col).cast(column.output_data_type)
                )
    return df


def convert_date_columns(df: DataFrame, unix_datetime: bool = False):
    def convert_col_to_date(df: DataFrame, column: str, unix_datetime: bool = False):
        if unix_datetime:
            df_converted = df.withColumn(column, F.to_date(F.from_unixtime(F.col(column).cast("string"), "yyyyMMdd"),"yyyyMMdd"))
        else:
            df_converted = df.withColumn(column, F.to_date(F.col(column).cast("string"), "yyyyMMdd"))
        return df_converted

    date_columns = list(filter(lambda x: "date" in x.lower() or "datum" in x.lower(), df.columns))
    for col in date_columns:
        df = convert_col_to_date(df, col, unix_datetime)
    return df

def add_column_comments(delta_path: str, keys, english_metadata_map, spark: SparkSession) -> None:
    for column in keys:
        comment = english_metadata_map[column]
        print(column, comment)
        comment = comment.replace("'", "")
        correct_column = to_snake_case(column)
        f_string = f"ALTER TABLE {delta_path} ALTER COLUMN {correct_column} COMMENT '{comment}'"
        spark.sql(f_string)

def chunked_download(url: str, output_path: str, chunk_size: int = 512 * 1024, max_chunks: int = None) -> None:
    response = requests.get(url, stream=True)
    response.raise_for_status()

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # /Volumes/main_catalog/rdw/raw
    with open(output_path, "wb") as f:
        chunks_downloaded = 0
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                f.write(chunk)
                chunks_downloaded += 1

                # Ingest smaller amount of data for testing
                if max_chunks and chunks_downloaded >= max_chunks:
                    print(f"[TEST MODE] Download limited to {chunks_downloaded} chunks")
                    break

def read_volume_csv(volume_path: str, spark: SparkSession = SparkSession.getActiveSession()) -> DataFrame: # rename type annottation into explicit spark DataFrame --> this is ambiguous, could be both pollars / pandas / spark?
    return (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(volume_path)
)

def read_delta_table(delta_path: str, spark: SparkSession = SparkSession.getActiveSession()) -> DataFrame: 
    return spark.read.table(delta_path)

def save_delta_table(df: DataFrame, delta_path: str, mode: str = "overwrite", mergeSchema: str = "false", overwriteSchema: str = "false") -> None:
    (
    df.write.format("delta")
    .mode(mode)
    .option("mergeSchema", mergeSchema) 
    .option("overwriteSchema", overwriteSchema)
    .saveAsTable(delta_path)


def get_latest_timestamp_folder(volume_base_path: str) -> str:
    """Find the latest timestamp folder in a volume directory.

    Args:
        volume_base_path: Base path to the volume (e.g., /Volumes/catalog/schema/volume)

    Returns:
        The latest timestamp string (e.g., '2025-12-01T09-00')

    Raises:
        FileNotFoundError: If no timestamp folders are found
    """
    if not os.path.exists(volume_base_path):
        raise FileNotFoundError(f"Volume path does not exist: {volume_base_path}")

    # List all directories that match timestamp pattern YYYY-MM-DDTHH-MM (allows single-digit hour/minute)
    timestamp_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{1,2}-\d{1,2}$")
    timestamp_folders = [
        d for d in os.listdir(volume_base_path)
        if os.path.isdir(os.path.join(volume_base_path, d)) and timestamp_pattern.match(d)
    ]

    if not timestamp_folders:
        raise FileNotFoundError(f"No timestamp folders found in: {volume_base_path}")

    # Parse timestamps for proper chronological sorting (handles non-zero-padded values)
    def parse_timestamp(folder: str):
        date_part, time_part = folder.split("T")
        hour, minute = time_part.split("-")
        return (date_part, int(hour), int(minute))

    latest = sorted(timestamp_folders, key=parse_timestamp)[-1]
    return latest