# RDW Data ETL Pipeline

A production-ready ETL pipeline for ingesting and processing Dutch RDW (Vehicle Authority) car registration data using Databricks, implementing the medallion architecture (Bronze/Silver/Gold) with SCD Type 2 change tracking.

## What's Built

### Core Pipeline Components

#### 1. **Data Ingestion (Bronze Layer)**
- Downloads CSV data from 10 RDW API endpoints covering:
  - Vehicle registrations (`gekentekende_voertuigen`)
  - Axle specifications (`gekentekende_voertuigen_assen`)
  - Fuel & emissions data (`gekentekende_voertuigen_brandstof`)
  - Body types (`gekentekende_voertuigen_carrosserie` and related tables)
  - Vehicle classes, subcategories, peculiarities
  - Mileage judgment explanations

- **Key Features**:
  - Chunked downloads with streaming support for large files
  - Raw data stored in Databricks volumes
  - Supports individual table or bulk downloads
  - Command: `download_rdw_data`

#### 2. **Data Cleaning & Transformation (Bronze Processing)**
- CSV to Delta table conversion with:
  - Column name standardization (snake_case)
  - Type conversions (DateType for yyyyMMdd formats, IntegerType, StringType)
  - Schema validation based on Pydantic models
  - Data quality filtering (e.g., license plate length validation)
  - Support for primary/foreign keys
  - Column descriptions/metadata

- **Key Features**:
  - Type hints throughout for IDE support
  - Constraint preservation (PKs/FKs)
  - Per-table or bulk processing
  - Command: `process_bronze_layer`

#### 3. **SCD Type 2 Implementation (Silver Layer)**
- Slowly Changing Dimensions Type 2 for historical tracking:
  - `__valid_from`: Timestamp when record became valid
  - `__valid_to`: Timestamp when record expired
  - `__is_current`: Boolean flag for current records
  - `__operation`: INSERT/UPDATE/DELETE
  - `__processed_time`: Pipeline execution timestamp

- **Key Features**:
  - Automatic change detection and history tracking
  - Full vs. incremental refresh modes
  - Preserves all historical states of vehicles
  - Uses internal `rdw.scd2` module for efficient processing
  - Command: `process_silver_layer`

#### 4. **Data Quality Framework (DQX)**
- **Databricks Labs DQX 0.8.0** integration for comprehensive data validation:
  - 10 YAML validation files (561 lines of rules)
  - Primary key integrity checks
  - Categorical enum validations
  - Numeric range validations
  - Business logic rules (e.g., max mass ≥ empty mass)
  - Quarantine mechanism for invalid records

- **Validation Categories**:
  - **Primary Keys**: NOT NULL on `kenteken`
  - **Required Columns**: NOT NULL on critical fields (`merk`, `handelsbenaming`, `voertuigsoort`)
  - **Categorical Values**: Enum checks (e.g., `wam_verzekerd` ∈ {Ja, Nee, N.v.t.})
  - **Numeric Ranges**:
    - `aantal_zitplaatsen`: 1-100
    - `aantal_cilinders`: 1-16
    - `massa_ledig_voertuig`: 50kg-50,000kg
  - **Business Rules**: Registration dates not in future, weight consistency

- **Key Features**:
  - Error vs. warning criticality levels
  - Valid/quarantined record splitting
  - Validation metrics logging
  - Per-table YAML configuration
  - Location: `src/rdw/dqx_checks/*.yml`

#### 5. **Gold Layer (Business Intelligence)**
- **Business-ready filtered dataset** from silver layer:
  - Source: `registered_vehicles` from silver layer
  - Filters applied:
    - Current records only (`__is_current == True`)
    - Non-exported vehicles (`export_indicator == "Nee"`)
    - Passenger cars only (`vehicle_type == "Personenauto"`)
    - Non-taxi vehicles (`taxi_indicator == "Nee"`)
    - WAM insured (`liability_insured == "Ja"`)
  - 50 selected columns for analytics

- **Output Tables**:
  - `registered_vehicles` - Filtered dataset (50 columns)
  - `registered_vehicles_dedup` - Deduplicated by make+trade_name (34 columns)
  - `vehicle_fleet_metrics` - Metric view for analytics

- **Key Features**:
  - Business rule filtering
  - Column selection for analytics use cases
  - Deduplication for unique vehicle models
  - YAML-based metric view creation
  - Command: `process_gold_layer`

#### 6. **Interactive Webapp**
- **Dash-based web application** for vehicle lookup and AI-powered search:
  - Bootstrap UI with responsive design
  - Real-time filtering (no database round-trips)
  - 5,000 records loaded into memory
  - AI chat integration via Databricks LLM endpoint

- **Features**:
  - **License Plate Lookup Tab**:
    - Brand/model/color dropdown filters
    - Engine size & weight range sliders
    - Grid/list view toggle
    - Individual vehicle detail pages (`/car/{kenteken}`)

  - **Prompt Feedback Tab**:
    - LLM prompt testing interface
    - Model/generation extraction validation
    - Feedback collection for prompt engineering

  - **AI Chat**:
    - Natural language vehicle queries
    - Context-aware responses
    - Conversation history management
    - Databricks serving endpoint integration

- **Tech Stack**:
  - Dash 2.18.2+ with Bootstrap components
  - Plotly for visualization
  - Polars & Pandas for data processing
  - Location: `webapp/`

#### 7. **Data Models & Schemas**
- **Pydantic BaseColumn**: Defines column transformations with:
  - Input/output column names
  - Type mappings (Python types → Spark types)
  - Primary/foreign key constraints
  - Nullable flags
  - Column descriptions

- **Pydantic RDWTable**: Extends BaseColumn with:
  - API URLs for data source
  - Computed properties for table paths (bronze, silver, gold)
  - DDL generation (CREATE TABLE statements)
  - Schema definitions

### Technology Stack

- **Platform**: Databricks (Unity Catalog with three catalogs)
- **Language**: Python 3.10+ with type hints
- **Data Processing**: PySpark, Delta Lake, Polars, Pandas
- **Data Validation**: Pydantic v2, Databricks Labs DQX 0.8.0
- **Change Tracking**: Slowly Changing Dimensions Type 2
- **AI/LLM**: Databricks LLM serving endpoints (webapp AI chat)
- **Web Framework**: Dash 2.18.2+ with Bootstrap components
- **Testing**: Pytest with local Spark session
- **CI/CD**: Azure DevOps (self-hosted Linux agents)
- **Package Management**: uv (Python package manager)

## Project Structure

```
├── src/rdw/                          # Main application code
│   ├── __init__.py                   # Version management
│   ├── conf.py                       # Configuration (catalogs, schemas, volumes)
│   ├── definitions.py                # RDW table definitions (10 tables, 116+ columns)
│   │
│   ├── jobs/                         # Executable jobs (5 scripts)
│   │   ├── download_rdw_data.py      # Download CSV from RDW API
│   │   ├── process_bronze_layer.py   # CSV → Delta conversion
│   │   ├── prepare_silver_layer.py   # Schema preparation for Silver
│   │   ├── process_silver_layer.py   # Bronze → Silver (SCD2 + DQX)
│   │   └── process_gold_layer.py     # Silver → Gold (aggregation)
│   │
│   ├── dqx_checks/                   # Data Quality validation
│   │   └── *.yml                     # YAML file per table
│   │
│   ├── scd2/                         # SCD2 processor module
│   │   └── processor.py              # SCD2 transformation logic
│   │
│   ├── views/                        # SQL view definitions
│   │   └── *.yml                     # Analytics view configurations
│   │
│   └── utils/                        # Utility functions
│       ├── models.py                 # Pydantic models (BaseColumn, RDWTable)
│       ├── utils.py                  # Data transformation functions
│       ├── spark_session.py          # Spark/Databricks session management
│       └── logging_config.py         # Logging configuration
│
├── webapp/                           # Interactive web application
│   ├── app.py                        # Main Dash application
│   ├── ai_chat.py                    # AI chat interface
│   ├── ai_car_chat.py                # Vehicle-specific AI chat
│   ├── components/                   # UI components
│   │   ├── filters.py                # Filter sidebar
│   │   ├── results.py                # Results display
│   │   ├── chat.py                   # Chat interface
│   │   └── layouts.py                # Page layouts
│   ├── callbacks/                    # Interactive callbacks
│   │   ├── routing_callbacks.py      # Page navigation
│   │   ├── filter_callbacks.py       # Filter logic
│   │   ├── results_callbacks.py      # Results updates
│   │   └── chat_callbacks.py         # AI chat handlers
│   ├── tabs/                         # Application tabs
│   │   ├── license_plate.py          # Vehicle lookup tab
│   │   ├── prompt_feedback.py        # LLM testing tab
│   │   └── common.py                 # Shared tab components
│   └── utils/                        # Helper functions
│       ├── data_loader.py            # Data loading utilities
│       ├── helpers.py                # Utility functions
│       └── styles.py                 # CSS styling
│
├── tests/                            # Test suite
│   ├── conftest.py                   # Pytest configuration
│   ├── spark_config.py               # Spark test configuration
│   ├── fixtures/
│   │   └── spark_fixture.py          # Local Spark session fixture
│   ├── unit_tests/
│   │   ├── test_bronze.py            # Bronze layer tests
│   │   ├── test_scd2.py              # SCD2 processor tests
│   │   ├── test_models.py            # Pydantic model tests
│   │   ├── test_utils.py             # Utility function tests
│   │   └── test_spark.py             # Spark session tests
│   └── integration_tests/
│       └── test_jobs_integration.py  # End-to-end job tests
│
├── devops/                           # Infrastructure & CI/CD
│   └── pipelines/
│       ├── azure-pipelines.yml       # Main CI/CD pipeline
│       ├── templates/                # Reusable pipeline templates
│       │   ├── jobs/
│       │   │   └── deploy-databricks-job.yml
│       │   └── steps/
│       │       ├── deploy-databricks-bundle.yml
│       │       └── install-databricks-cli.yml
│       └── variables/
│           └── pipeline-statics.yml  # Pipeline variables
│
├── .claude/                          # Claude AI agent configurations
│   ├── agents/                       # Specialized agent configs
│   └── *.md                          # Development guides
│
├── dashboard/                        # Databricks dashboard configs
├── dist/                             # Built distribution packages
│
├── pyproject.toml                    # Project configuration & dependencies
├── databricks.yml                    # Databricks Asset Bundle configuration
├── taskfile.yml                      # Task runner automation
├── version.txt                       # Version string (0.2)
├── CLAUDE.md                         # Implementation guidelines
└── README.md                         # This file
```

## Getting Started

### Prerequisites

- Python 3.10+
- Databricks workspace with Unity Catalog enabled
- Three catalogs: `bronze_catalog`, `silver_catalog`, `gold_catalog`
- RDW schema: `rdw_etl` in each catalog
- Volumes for raw data: `/Volumes/bronze_catalog/rdw_etl/raw`

### Installation

```bash
# Clone the repository
git clone <repo-url>
cd rdw_blogs_repo

# Create virtual environment using uv
uv venv
source .venv/bin/activate  # or `.venv\Scripts\activate` on Windows

# Install in development mode
uv pip install -e ".[dev]"

# Run tests
pytest tests/
```

### Core Dependencies

**Minimal** (production):
```toml
pydantic_settings>=1.10.6
databricks-labs-dqx==0.8.0  # Data quality validation
```

**Development**:
```toml
pandas==2.2.3
numpy==1.26.4
databricks-connect==15.4.5
pytest>=8.3.5
pre-commit>=4.1.0
ipykernel>=6.29.5
```

**Testing**:
```toml
delta-spark==3.3.0
pyspark==3.5.5
pytest-cov>=6.1.0
pytest-order>=1.2.0
requests>=2.31.0
```

**Webapp**:
```toml
dash>=2.18.2                      # Web framework
dash-bootstrap-components>=1.6.0  # UI components
polars>=1.0.0                     # Data processing
huggingface-hub>=0.26.2           # LLM integration
```

## Usage

### 1. Download RDW Data

```bash
# Download all tables
download_rdw_data

# Download specific table
download_rdw_data --table-name gekentekende_voertuigen

# With environment selection
download_rdw_data --environment DEV
```

### 2. Process Bronze Layer (CSV → Delta)

```bash
# Process all tables
process_bronze_layer

# Process specific table
process_bronze_layer --table-name gekentekende_voertuigen

# With environment
process_bronze_layer --environment DEV
```

### 3. Process Silver Layer (SCD Type 2 + DQX)

```bash
# Incremental processing (recommended)
process_silver_layer

# Full refresh (overwrites history)
process_silver_layer --full-refresh true

# Specific table
process_silver_layer --table-name gekentekende_voertuigen
```

### 4. Process Gold Layer (Business Intelligence)

```bash
# Process gold layer (filters silver data and creates analytics tables)
process_gold_layer
```

### 5. Run Interactive Webapp

```bash
# Start the Dash web application
cd webapp
python app.py

# Access at http://localhost:8050
# Features:
#  - License Plate Lookup: Browse vehicles with filters
#  - Prompt Feedback: Test LLM prompts for model/generation extraction
#  - AI Chat: Natural language vehicle queries
```

## Data Architecture

### Medallion Layers

#### Bronze Layer
- **Purpose**: Raw data ingestion (landing zone)
- **Tables**: 10 RDW tables with 116+ total columns
- **Primary Key**: `kenteken` (Dutch license plate)
- **Format**: Delta tables in Databricks
- **Schema**: Standardized column names, preserved data types
- **Catalog**: `bronze_catalog.rdw_etl`

#### Silver Layer
- **Purpose**: Cleaned, validated, historicized data
- **SCD2 Columns**:
  - `__valid_from`: Record validity start timestamp
  - `__valid_to`: Record validity end timestamp
  - `__is_current`: Boolean for active records
  - `__operation`: Change type (INSERT/UPDATE/DELETE)
  - `__processed_time`: Processing timestamp
- **Catalog**: `silver_catalog.rdw_etl`
- **Processing**: Automatic change detection vs. previous snapshot

#### Gold Layer
- **Purpose**: Business-ready filtered dataset & analytics views
- **Status**: Production Ready
- **Tables**:
  - `registered_vehicles` - Filtered passenger cars (50 columns)
  - `registered_vehicles_dedup` - Deduplicated by make+trade_name (34 columns)
  - `vehicle_fleet_metrics` - YAML-based metric view
- **Features**:
  - Business rule filtering (non-exported, non-taxi, insured vehicles)
  - Column selection for analytics
  - Deduplication for unique models
- **Catalog**: `gold_catalog.rdw_etl`

### Table Definitions (10 RDW Tables)

| Table Name | Primary Key | Foreign Keys | Columns | Purpose |
|------------|------------|--------------|---------|---------|
| `registered_vehicles` | `license_plate` | - | 116 | Main vehicle registrations |
| `registered_vehicles_axles` | `license_plate`, `axle_number` | `license_plate` | 10 | Axle specifications |
| `registered_vehicles_fuel` | `license_plate`, `fuel_sequence_number` | `license_plate` | 35+ | Fuel & emissions |
| `registered_vehicles_body` | `license_plate`, `body_sequence_number` | `license_plate` | 4 | Body type info |
| `registered_vehicles_body_specification` | `license_plate`, etc. | `license_plate` | 5 | Detailed body specs |
| `registered_vehicles_class` | `license_plate`, etc. | `license_plate` | 5 | Vehicle classes |
| `registered_vehicles_subcategory` | `license_plate`, etc. | `license_plate` | 4 | Vehicle subcategories |
| `registered_vehicles_special_features` | `license_plate`, etc. | `license_plate` | 6 | Special characteristics |
| `registered_vehicles_crawler_tracks` | `license_plate`, etc. | `license_plate` | 7 | Crawler track info |
| `odometer_reading_judgment_explanation` | `explanation_code` | - | 2 | Mileage judgment explanations |

## Testing

### Setup

Before running tests, sync test fixtures from Databricks:

```bash
# Download test data fixtures (required before first test run)
task sync-test
```

### Unit Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/unit_tests/test_bronze.py

# Run with coverage
pytest --cov=src tests/

# Run excluding CI-expensive tests
pytest -m "not ci_exclude"
```

### Test Coverage

- **Local Spark Tests**: `test_spark_fixture`, `test_spark.py`
- **Bronze Layer**: Schema validation, column conversion tests
- **SCD2**: Change detection and history tracking tests
- **Models**: Pydantic model validation tests
- **Utils**: Utility function tests
- **Integration**: End-to-end job workflow tests

### Local Testing

Tests use a local Spark session configured in `tests/fixtures/spark_fixture.py`:
- Master: `local[1]`
- SQL shuffle partitions: 1
- Executor cores: 1

## CI/CD Pipeline

### Azure DevOps Integration

**Pipeline**: `devops/pipelines/azure-pipelines.yml`

**Stages**:
1. **CI (Continuous Integration)**
   - Code checkout
   - Disk space verification
   - uv package manager installation
   - Java installation (for PySpark)
   - Virtual environment setup
   - Dependency installation
   - Linting (ruff)
   - Type checking (mypy)
   - Unit tests (pytest)

2. **CD (Continuous Deployment)**
   - Databricks bundle deployment
   - Job scheduling

**Triggers**:
- On commits to `main` and `feature/*` branches
- On pull requests
- Excludes documentation changes (*.md, docs/**)

**Agent Pool**: Self-hosted Linux agents

## Configuration

### Environment Variables

The pipeline supports DEV/ACC/PRD environments through:
- `DATABRICKS_HOST_DEV`, `DATABRICKS_HOST_ACC`, `DATABRICKS_HOST_PRD`
- Job parameters: `--environment DEV|ACC|PRD`

### Databricks Configuration

Required setup:
```
Catalogs:
  - bronze_catalog
  - silver_catalog
  - gold_catalog

Schema:
  - rdw_etl (in each catalog)

Volumes:
  - /Volumes/bronze_catalog/rdw_etl/raw  (for CSV storage)
```

### Databricks Asset Bundle

The `databricks.yml` configures:
- **Bundle**: RDW_ETL
- **Variables**:
  - `environment`: DEV/ACC/PRD switching
  - `full_refresh`: Overwrite flag
  - `rdw_table_names`: Array of 10 tables
  - `job_concurrency`: Parallel task limit (default 4)
- **Jobs**:
  - `rdw_download`: Downloads all tables in parallel
  - `bronze`: Bronze layer processing
  - Wheel package deployment
  - Environment-specific dependencies

## Future Roadmap

**Potential Enhancements**:
- [ ] Data lineage tracking
- [ ] Quality metrics dashboards
- [ ] Advanced monitoring and alerting
- [ ] Performance optimization for full-scale processing

## Performance

### Sample Scale (1,000 records)
- CSV download: ~2-5 seconds
- Bronze processing: ~10 seconds
- Silver SCD2 processing: ~5-10 seconds

### Estimated Full Scale (16.6M RDW records)
- Bronze ingestion: ~30-60 minutes
- Silver SCD2 processing: ~15-30 minutes
- Total pipeline run: ~1-2 hours

## Quality Assurance

### Code Quality

- **Linting**: Ruff with strict rules (line-length: 120, includes D, ANN, E, W, B, I, SIM)
- **Type Checking**: mypy-compatible (all functions have type hints)
- **Documentation**: Docstrings for all public functions/classes
- **Testing**: Unit tests with pytest, local Spark fixtures

### Data Quality

- Schema validation via Pydantic
- Column type verification
- License plate length validation (6 chars for Dutch plates)
- Primary/foreign key constraints
- NULL handling via `is_nullable` flags
- DQX validation with quarantine mechanism

## Contributing

### Development Workflow

1. Create feature branch from `main`
2. Make changes with type hints
3. Add tests for new functionality
4. Run linting: `ruff check .`
5. Run tests: `pytest`
6. Submit PR for review

### Code Standards

- Type hints on all functions: `def func(x: str) -> int:`
- Docstrings following Google style
- Black-compatible formatting (line-length: 120)
- No circular imports
- Avoid hardcoding paths (use `conf.py`)

## Documentation

### In Code
- CLAUDE.md: Mission objectives & implementation guidelines
- Inline docstrings for all functions/classes
- Type hints for IDE support

### In Repository
- `.claude/`: Development guides for medallion architecture, testing strategy, Spark setup
- `devops/`: Pipeline configuration documentation

## Support & Issues

### Common Issues

**Issue**: "No access to table in silver_catalog"
- **Solution**: Grant Service Principal read access to tables in Databricks UI

**Issue**: "Java not found" (during tests)
- **Solution**: Install OpenJDK 11: `sudo apt-get install openjdk-11-jdk`


## License

MIT Licensed 

Built by Cauchy.io, 2025.
