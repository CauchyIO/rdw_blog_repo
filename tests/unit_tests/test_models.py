import pytest
from pyspark.sql.types import StringType, IntegerType, DateType

from src.rdw.utils.models import BaseColumn, BaseTable, RDWTable


def test_base_column_creation():
    """Test BaseColumn model validation and properties."""
    col = BaseColumn(
        input_col="Kenteken",
        output_col="kenteken",
        original_type=str,
        output_data_type=StringType(),
        is_nullable=False,
        is_primary_key=True,
        description="License plate identifier"
    )

    assert col.input_col == "Kenteken"
    assert col.output_col == "kenteken"
    assert col.is_nullable is False
    assert col.is_primary_key is True
    assert col.comment == "License plate identifier"


def test_base_column_comment_strips_quotes():
    """Test that comment property removes single quotes."""
    col = BaseColumn(
        input_col="Test",
        output_col="test",
        original_type=str,
        output_data_type=StringType(),
        description="This is a 'test' description"
    )

    assert col.comment == "This is a test description"


def test_base_column_foreign_key():
    """Test BaseColumn foreign key configuration."""
    col = BaseColumn(
        input_col="Code",
        output_col="code",
        original_type=int,
        output_data_type=IntegerType(),
        is_foreign_key=True,
        foreign_key_reference_table="reference_table",
        foreign_key_reference_column="id"
    )

    assert col.is_foreign_key is True
    assert col.foreign_key_reference_table == "reference_table"
    assert col.foreign_key_reference_column == "id"


def test_base_table_create_statement_bronze():
    """Test CREATE TABLE statement generation for bronze layer (no SCD2 columns)."""
    table = BaseTable(
        name="test_vehicles",
        description="Test vehicles table",
        columns=[
            BaseColumn(input_col="Kenteken", output_col="kenteken",
                      original_type=str, output_data_type=StringType(),
                      is_nullable=False, is_primary_key=True),
            BaseColumn(input_col="Merk", output_col="merk",
                      original_type=str, output_data_type=StringType()),
            BaseColumn(input_col="Bouwjaar", output_col="bouwjaar",
                      original_type=int, output_data_type=IntegerType()),
        ]
    )

    statement = table.create_table_statement(catalog="bronze_catalog", schema="rdw", scd2_cols=False)

    assert "CREATE TABLE IF NOT EXISTS bronze_catalog.rdw.test_vehicles" in statement
    assert "kenteken STRING NOT NULL PRIMARY KEY" in statement
    assert "merk STRING" in statement
    assert "bouwjaar INTEGER" in statement
    assert "USING DELTA" in statement
    # Should not have SCD2 columns
    assert "__valid_from" not in statement
    assert "__is_current" not in statement


def test_base_table_create_statement_silver_with_scd2():
    """Test CREATE TABLE statement generation for silver layer (with SCD2 columns)."""
    table = BaseTable(
        name="test_vehicles",
        description="Test vehicles table",
        columns=[
            BaseColumn(input_col="Kenteken", output_col="kenteken",
                      original_type=str, output_data_type=StringType(),
                      is_nullable=False, is_primary_key=True),
            BaseColumn(input_col="Merk", output_col="merk",
                      original_type=str, output_data_type=StringType()),
        ]
    )

    statement = table.create_table_statement(catalog="silver_catalog", schema="rdw", scd2_cols=True)

    assert "CREATE TABLE IF NOT EXISTS silver_catalog.rdw.test_vehicles" in statement
    assert "kenteken STRING NOT NULL PRIMARY KEY" in statement
    assert "__valid_from TIMESTAMP" in statement
    assert "__valid_to TIMESTAMP" in statement
    assert "__is_current BOOLEAN" in statement
    assert "__operation STRING" in statement
    assert "__processed_time TIMESTAMP" in statement


def test_base_table_create_statement_with_foreign_key():
    """Test CREATE TABLE statement with foreign key constraints."""
    table = BaseTable(
        name="test_fuel",
        description="Test fuel table",
        columns=[
            BaseColumn(input_col="Kenteken", output_col="kenteken",
                      original_type=str, output_data_type=StringType(),
                      is_foreign_key=True,
                      foreign_key_reference_table="vehicles",
                      foreign_key_reference_column="kenteken"),
            BaseColumn(input_col="Brandstof", output_col="brandstof",
                      original_type=str, output_data_type=StringType()),
        ]
    )

    statement = table.create_table_statement(catalog="bronze_catalog", schema="rdw")

    assert "FOREIGN KEY (kenteken) REFERENCES bronze_catalog.rdw.vehicles(kenteken)" in statement


def test_base_table_alter_comment_statements():
    """Test ALTER TABLE statements for column comments."""
    table = BaseTable(
        name="test_vehicles",
        description="Test vehicles table",
        columns=[
            BaseColumn(input_col="Kenteken", output_col="kenteken",
                      original_type=str, output_data_type=StringType(),
                      description="Vehicle license plate"),
            BaseColumn(input_col="Merk", output_col="merk",
                      original_type=str, output_data_type=StringType(),
                      description="Vehicle brand"),
        ]
    )

    statements = table.alter_comment_statements(catalog="bronze_catalog", schema="rdw")

    # First statement is table-level comment, then column comments
    assert len(statements) == 3
    assert 'SET TBLPROPERTIES' in statements[0]
    assert 'ALTER TABLE bronze_catalog.rdw.test_vehicles ALTER COLUMN kenteken COMMENT "Vehicle license plate"' in statements[1]
    assert 'ALTER TABLE bronze_catalog.rdw.test_vehicles ALTER COLUMN merk COMMENT "Vehicle brand"' in statements[2]


def test_rdw_table_computed_paths():
    """Test RDWTable computed field properties for volume and delta paths."""
    table = RDWTable(
        url="https://opendata.rdw.nl/api/views/test.csv",
        name="gekentekende_voertuigen",
        description="Test table description",
        columns=[
            BaseColumn(input_col="Kenteken", output_col="kenteken",
                      original_type=str, output_data_type=StringType(),
                      is_primary_key=True, is_nullable=False),
        ]
    )

    # Check volume path
    assert "gekentekende_voertuigen.csv" in table.volume_file_path
    assert "/Volumes/" in table.volume_file_path

    # Check delta paths (schema is rdw_etl)
    assert table.delta_bronze_path.endswith(".rdw_etl.gekentekende_voertuigen")
    assert table.delta_silver_path.endswith(".rdw_etl.gekentekende_voertuigen")
    assert table.delta_gold_path.endswith(".rdw_etl.gekentekende_voertuigen")


def test_rdw_table_timestamped_volume_path():
    """Test RDWTable get_timestamped_volume_path method for historic data retention."""
    table = RDWTable(
        url="https://opendata.rdw.nl/api/views/test.csv",
        name="gekentekende_voertuigen",
        description="Test table description",
        columns=[
            BaseColumn(input_col="Kenteken", output_col="kenteken",
                      original_type=str, output_data_type=StringType(),
                      is_primary_key=True, is_nullable=False),
        ]
    )

    timestamp = "2025-12-01T09-00-00"
    timestamped_path = table.get_timestamped_volume_path(timestamp)

    # Check that timestamp is included in the path
    assert timestamp in timestamped_path
    assert "/Volumes/" in timestamped_path
    assert "gekentekende_voertuigen.csv" in timestamped_path

    # Check path structure: /Volumes/{catalog}/{schema}/{volume}/{timestamp}/{table}.csv
    assert f"/{timestamp}/" in timestamped_path

    # Compare with default volume_file_path - should differ only by timestamp folder
    default_path = table.volume_file_path
    assert default_path != timestamped_path
    assert timestamped_path.replace(f"/{timestamp}", "") == default_path


def test_base_table_schema_definition():
    """Test StructType schema generation from BaseTable."""
    table = BaseTable(
        name="test_table",
        description="Test table",
        columns=[
            BaseColumn(input_col="Col1", output_col="col1",
                      original_type=str, output_data_type=StringType(), is_nullable=False),
            BaseColumn(input_col="Col2", output_col="col2",
                      original_type=int, output_data_type=IntegerType(), is_nullable=True),
            BaseColumn(input_col="Col3", output_col="col3",
                      original_type=str, output_data_type=DateType(), is_nullable=True),
        ]
    )

    schema = table.schema_definition

    # Check field names
    field_names = [field.name for field in schema.fields]
    assert field_names == ["col1", "col2", "col3"]

    # Check nullable flags
    assert schema.fields[0].nullable is False  # col1 not nullable
    assert schema.fields[1].nullable is True   # col2 nullable
    assert schema.fields[2].nullable is True   # col3 nullable

    # Check data types
    assert isinstance(schema.fields[0].dataType, StringType)
    assert isinstance(schema.fields[1].dataType, IntegerType)
    assert isinstance(schema.fields[2].dataType, DateType)