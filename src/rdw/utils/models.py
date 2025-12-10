from pydantic import BaseModel, computed_field
# from pydantic.functional_validators import computed_field
from pyspark.sql.types import StructType, StructField, DataType
from typing import Type
from pathlib import Path
from rdw.conf import bronze_catalog, silver_catalog, gold_catalog, ingestion_volume, rdw_schema

class BaseColumn(BaseModel):
    input_col: str
    output_col: str
    original_type: Type
    output_data_type: DataType
    is_nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_key_reference_table: str = None 
    foreign_key_reference_column: str = None 

    @property
    def comment(self):
        return self.description.replace("'", "")

    # required to address the pydantic error raised: Unable to generate pydantic-core schema for <class 'pyspark.sql.types.DataType'>.
    model_config = {"arbitrary_types_allowed": True}


class BaseTable(BaseModel):
    name: str
    description: str
    columns: list[BaseColumn]

    @property
    def primary_key_column(self) -> str | None:
        """Get the primary key column name, or None if not defined."""
        for col in self.columns:
            if col.is_primary_key:
                return col.output_col
        return None

    def create_table_statement(self, catalog: str = bronze_catalog, schema: str = rdw_schema, scd2_cols: bool = False, foreign_keys: bool = True) -> str:
        type_mapping = {
            'StringType': 'STRING',
            'DateType': 'DATE',
            'IntegerType': 'INTEGER'
        }
        
        fk_statements = []
        create_statement = f"CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{self.name} (\n"

        for col in self.columns:
            spark_type_name = col.output_data_type.__class__.__name__
            sql_type = type_mapping.get(spark_type_name) 
            nullable = "NOT NULL" if col.is_nullable is False else ""
            fk_reference_col = col.foreign_key_reference_column if col.foreign_key_reference_column is not None else col.output_col

            if col.is_primary_key: # primary key column
                create_statement += f"\t{col.output_col} {sql_type} {nullable} PRIMARY KEY,\n"

            elif col.is_foreign_key: # foreign key column
                create_statement += f"\t{col.output_col} {sql_type} {nullable},\n"
                fk_statements.append(f"\tFOREIGN KEY ({col.output_col}) REFERENCES {catalog}.{schema}.{col.foreign_key_reference_table}({fk_reference_col}),\n")

            else: # normal column
                create_statement += f"\t{col.output_col} {sql_type} {nullable},\n"   
        
        # Add the SCD2 columns if specified
        if scd2_cols:
            scd2_columns = [
                    "__valid_from TIMESTAMP",
                    "__valid_to TIMESTAMP",
                    "__is_current BOOLEAN",
                    "__operation STRING",
                    "__processed_time TIMESTAMP"
                ]
            for scd_col in scd2_columns:
                create_statement += f"\t{scd_col},\n"

        if foreign_keys:
            # Add the FK statements. They have to be last
            create_statement += ''.join(fk_statements)

        # Remove the last "," for correct syntax.
        create_statement = create_statement[:create_statement.rfind(',')] + create_statement[create_statement.rfind(',')+1:]

        create_statement += ") USING DELTA;"
        return create_statement

    def alter_comment_statements(self, catalog: str = bronze_catalog, schema: str = rdw_schema, scd2_cols: bool = False) -> str:
        # Use an array because Spark.SQL() can only process one ALTER statement at a time.
        alter_statements = []

        # Add table-level comment 
        table_comment = self.description.replace("'", "")
        alter_statements.append(f'ALTER TABLE {catalog}.{schema}.{self.name} SET TBLPROPERTIES ("comment" = "{table_comment}");\n')

        # add comments for each col
        for col in self.columns:
            alter_statements.append(f'ALTER TABLE {catalog}.{schema}.{self.name} ALTER COLUMN {col.output_col} COMMENT "{col.comment}";\n')

        # Add comments for SCD2 columns if specified
        if scd2_cols:
            scd2_column_comments = [
                ("__valid_from", "SCD2: Timestamp when this record version became valid"),
                ("__valid_to", "SCD2: Timestamp when this record version became invalid (NULL for current records)"),
                ("__is_current", "SCD2: Boolean flag indicating if this is the current version of the record"),
                ("__operation", "SCD2: Type of operation that created this record (INSERT, UPDATE, DELETE)"),
                ("__processed_time", "SCD2: Timestamp when this record was processed into the table")
            ]
            for col_name, comment in scd2_column_comments:
                alter_statements.append(f'ALTER TABLE {catalog}.{schema}.{self.name} ALTER COLUMN {col_name} COMMENT "{comment}";\n')

        return alter_statements
    
    def alter_fk_statements(self, catalog: str = bronze_catalog, schema: str = rdw_schema):
        alter_statements = []
        for col in self.columns:
            if col.is_foreign_key:
                fk_reference_col = col.foreign_key_reference_column if col.foreign_key_reference_column is not None else col.output_col
                constraint_name = f"{self.name}_{col.output_col}_fk_to_{col.foreign_key_reference_table}"

                alter_statements.append({
                    "constraint_name": constraint_name,
                    "statement": f"ALTER TABLE {catalog}.{schema}.{self.name} ADD CONSTRAINT {constraint_name} FOREIGN KEY ({col.output_col}) REFERENCES {catalog}.{schema}.{col.foreign_key_reference_table}({fk_reference_col});"
                })

        return alter_statements

    def check_fk_exists_query(self, constraint_name: str, catalog: str = bronze_catalog, schema: str = rdw_schema) -> str:
        return f"""
            SELECT constraint_name
            FROM {catalog}.information_schema.table_constraints
            WHERE table_catalog = '{catalog}'
              AND table_schema = '{schema}'
              AND table_name = '{self.name}'
              AND constraint_name = '{constraint_name}'
              AND constraint_type = 'FOREIGN KEY'
        """

    @computed_field
    @property
    def schema_definition(self) -> str:
        StructFields = []

        for col in self.columns:
            sf = StructField(col.output_col, col.output_data_type, col.is_nullable)
            StructFields.append(sf)

        return StructType(StructFields)
    
class RDWTable(BaseTable):
    url: str
    database: str = rdw_schema

    @computed_field
    @property
    def dqx_checks_path(self) -> str:
        # Get path relative to the rdw package root
        package_root = Path(__file__).parent.parent
        return str(package_root / "dqx_checks" / f"{self.name}.yml")

    @computed_field
    @property
    def volume_file_path(self) -> str:
        return f"/Volumes/{bronze_catalog}/{self.database}/{ingestion_volume}/{self.name}.csv"

    def get_timestamped_volume_path(self, timestamp: str) -> str:
        """Generate volume path with timestamp subfolder for historic data retention."""
        return f"/Volumes/{bronze_catalog}/{self.database}/{ingestion_volume}/{timestamp}/{self.name}.csv"

    @computed_field
    @property
    def volume_base_path(self) -> str:
        """Base path to the volume directory (without table name)."""
        return f"/Volumes/{bronze_catalog}/{self.database}/{ingestion_volume}" 

    @computed_field
    @property
    def delta_bronze_path(self) -> str:
        return f"{bronze_catalog}.{self.database}.{self.name}"
    
    @computed_field
    @property
    def delta_silver_path(self) -> str:
        return f"{silver_catalog}.{self.database}.{self.name}"
    
    @computed_field
    @property
    def delta_gold_path(self) -> str:
        return f"{gold_catalog}.{self.database}.{self.name}"