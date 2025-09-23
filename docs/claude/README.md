# Data-Check Project Documentation

## Overview

Data-Check is a comprehensive data quality and comparison tool designed to facilitate easy data diffing between two BigQuery tables, views, or SQL queries. This project enables data engineers and analysts to understand data quality changes when switching sources, validate query impacts, and ensure data consistency across different environments or transformations.

## Project Goal

The primary objective of Data-Check is to provide an intuitive interface for comparing datasets to:
- **Validate data quality** when changing data sources
- **Understand the impact** of queries or transformations on results
- **Identify discrepancies** between development, staging, and production datasets
- **Monitor data consistency** across different time periods or data pipelines

## Architecture

### Core Components

#### 1. Data Processor (`data_check/data_processor.py`)
- **Abstract base class** defining the interface for data comparison operations
- Handles input validation (SQL queries vs table names)
- Manages primary key configuration and sampling rates
- Provides methods for schema comparison and data diffing

#### 2. BigQuery Processor (`data_check/processors/bigquery.py`)
- **Concrete implementation** of DataProcessor for Google BigQuery
- Supports complex primary key combinations (single or multiple columns)
- Implements sampling for large datasets
- Generates optimized SQL queries for data comparison

#### 3. Data Models (`data_check/models/table.py`)
- **TableSchema**: Represents table structure with column metadata
- **ColumnSchema**: Defines individual column properties (name, type, mode)
- **BigQueryDataType**: Enum for supported BigQuery data types
- Handles complex data types (ARRAY, STRUCT, RECORD) appropriately

#### 4. Query Client (`data_check/query_client.py`)
- **Abstract interface** for database interactions
- **QueryBigQuery** (`data_check/query/query_bq.py`): BigQuery-specific implementation
- Executes SQL queries and returns structured results

#### 5. Streamlit Interface (`data_check/streamlit_app.py`)
- **Interactive web application** for data comparison
- Two-step configuration process:
  1. Table/query selection and sampling rate
  2. Primary key and column selection
- Real-time data quality metrics and visualizations

#### 6. Data Formatter (`data_check/data_formatter.py`)
- **Styling utilities** for highlighting differences in Streamlit
- Gradient visualizations for data quality metrics
- Row-by-row difference highlighting

### Key Features

#### Multi-Step Data Comparison Process

1. **Schema Analysis**
   - Identifies common and exclusive columns between datasets
   - Warns about unsupported data types (RECORD fields)
   - Validates column type compatibility

2. **Primary Key Validation**
   - Ensures primary key uniqueness across both datasets
   - Supports single or composite primary keys
   - Provides detailed error messages for constraint violations

3. **Data Quality Metrics**
   - Calculates percentage of different values per column
   - Shows ratio of non-null values
   - Displays missing primary key statistics

4. **Row-Level Differences**
   - Identifies exact rows where data differs
   - Provides side-by-side comparison (column__1 vs column__2)
   - Supports pagination for large result sets
   - Generates SQL queries for further investigation

#### Advanced Capabilities

- **Sampling Support**: Configurable sampling rates for large tables (10-100%)
- **Complex Data Types**: Handles BigQuery ARRAY and STRUCT types with appropriate casting
- **Performance Optimization**: Uses CTEs and optimized joins for efficient query execution
- **URL Parameter Support**: Enables bookmarking and sharing of specific comparisons

## Technical Implementation

### Dependencies
- **Core**: Python 3.9+, pandas, sqlglot
- **BigQuery**: google-cloud-bigquery, pandas-gbq
- **UI**: Streamlit, seaborn for visualizations
- **Development**: pytest, ruff, isort

### Database Support
Currently supports **Google BigQuery** with an extensible architecture for additional databases:
- Dialect-specific SQL generation using sqlglot
- Abstract interfaces for easy database adapter implementation

### Query Generation
Uses **sqlglot** for:
- Cross-dialect SQL generation
- Query parsing and validation
- Expression building for complex comparisons

## Use Cases

### 1. Data Migration Validation
Compare data before and after migrating between systems:
```sql
-- Old system
SELECT * FROM legacy_db.users
-- New system
SELECT * FROM modern_db.users
```

### 2. ETL Pipeline Testing
Validate transformations during development:
```sql
-- Source data
SELECT * FROM raw.customer_events
-- Transformed data
SELECT * FROM processed.customer_metrics
```

### 3. A/B Testing Analysis
Compare different algorithm outputs:
```sql
-- Control group results
SELECT * FROM ml_predictions_v1
-- Test group results
SELECT * FROM ml_predictions_v2
```

### 4. Data Quality Monitoring
Regular checks between environments:
```sql
-- Production
SELECT * FROM prod.daily_aggregates
-- Staging
SELECT * FROM staging.daily_aggregates
```

## Deployment

### Local Development
```bash
pip install -r requirements.txt
pip install -e .
streamlit run data_check/streamlit_app.py
```

### Docker Deployment
```bash
docker build -t data-check .
docker run -p 8501:8501 data-check
```

### Testing
```bash
pytest tests/
```

## Future Enhancements

- **Multi-database support** (PostgreSQL, Snowflake, etc.)
- **Automated scheduling** for regular data quality checks
- **Alert notifications** for data quality degradation
- **Historical tracking** of data quality metrics
- **Export capabilities** (CSV, JSON, reports)
- **API interface** for programmatic access

## Contributing

The project follows a clean architecture pattern with:
- Abstract interfaces for extensibility
- Comprehensive test coverage
- Type hints throughout the codebase
- Modular design for easy maintenance

Data-Check serves as a powerful alternative to commercial tools like Datafold's Data-Diff, providing essential data quality validation capabilities in an open-source, customizable package.