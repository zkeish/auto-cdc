# auto-cdc

**Automated Change Data Capture (CDC) framework for Databricks Delta Lake**

A Python package that simplifies implementing Change Data Capture patterns on Databricks Delta Lake, enabling efficient tracking and management of data changes.

## Features

- **Automated CDC Management**: Seamlessly write change data to Delta tables with built-in change tracking
- **Schema Evolution**: Automatically handle schema changes between source and target tables
- **Version Management**: Track and manage multiple CDC versions with timestamp-based versioning
- **Data Cleanup**: Built-in vacuum and retention policies for managing table history
- **Merge Operations**: Configurable merge logic with support for custom change detection
- **Change Feed Integration**: Native integration with Delta Change Data Feed for efficient CDC consumption

## Installation

```bash
pip install auto-cdc
```

### From Source

```bash
git clone https://github.com/yourusername/auto-cdc.git
cd auto-cdc
pip install -e ".[dev]"
```

## Docker

Build the project image:

```bash
docker build -t auto-cdc .
```

Run an interactive shell inside the container:

```bash
docker run --rm -it -v "$PWD":/app -w /app auto-cdc bash
```

Using Docker Compose:

```bash
docker compose up --build
```

Run the CSV ingestion example via Docker Compose:

```bash
docker compose run --rm app python examples/data_feed.py /path/to/data.csv /Volumes/catalog.schema/table --keys id
```

## Quick Start

### Basic Usage

```python
from auto_cdc import CDC
from datetime import datetime

# Write data to CDC feed
CDC.write_to_cdc_feed(
    spark_dataframe=df,
    folder_path="Volumes/catalog.schema/cdc_folder",
    keys=["id"],  # Key columns for identifying rows
    source_timestamp=datetime.now(),
    vacuum_days=7,
)
```

### Advanced Configuration

```python
CDC.write_to_cdc_feed(
    spark_dataframe=df,
    folder_path="Volumes/catalog.schema/cdc_folder",
    keys=["order_id", "customer_id"],
    source_timestamp=datetime.now(),
    exclude_columns_from_tracking=["updated_at", "etl_timestamp"],
    vacuum_days=30,
    recurse=True,
    batch=True,
)
```

## Parameters

### `CDC.write_to_cdc_feed()`

- **spark_dataframe** (DataFrame): Input Spark dataframe with changes
- **folder_path** (str): Target folder path for CDC data (Volumes path)
- **keys** (List[str]): List of column names that uniquely identify rows
- **source_timestamp** (datetime): Timestamp indicating when the data was sourced
- **exclude_columns_from_tracking** (Optional[List[str]]): Columns to ignore in change detection
- **vacuum_days** (int): Number of days to retain before vacuuming (default: 7)
- **recurse** (bool): Handle out-of-sequence inserts by rewriting feed (default: True)
- **batch** (bool): Use batch mode for writes (default: True)

## API Reference

### CDC

Main class for CDC operations.

#### Methods

- `write_to_cdc_feed()`: Write data to CDC feed with change tracking
- `delete_cdc_feed()`: Delete a CDC feed and associated tables
- `cdc_tracker_table_name()`: Get the CDC tracker table name from a folder path

### Exceptions

- `CDCException`: Base exception for all CDC operations
- `CDCVersionError`: Raised when there's an issue with CDC versions
- `CDCSchemaError`: Raised when there's a schema mismatch or evolution issue
- `CDCFileError`: Raised when there's an issue with file operations

## How It Works

1. **First Load**: Initializes the target Delta table and creates a helper table for version tracking
2. **Subsequent Loads**: 
   - Detects schema changes and evolves table structure as needed
   - Identifies changed rows using key columns
   - Performs MERGE operation to insert new records and update existing ones
   - Automatically deletes records that no longer exist in source
3. **Change Feed**: Outputs all changes to volume as parquet files with proper naming convention
4. **Cleanup**: Periodically vacuums old table versions based on retention policy

## Requirements

- Python 3.8+
- PySpark 3.4+
- Databricks Runtime 11.3+
- Delta Lake 3.0+
- Databricks SDK

## Development

### Setting Up Development Environment

```bash
# Clone repository
git clone https://github.com/yourusername/auto-cdc.git
cd auto-cdc

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install development dependencies
pip install -e ".[dev]"
```

### Running Tests

```bash
pytest
pytest --cov  # With coverage report
```

### Code Quality

```bash
# Format code
black src/

# Lint
ruff check src/

# Type checking
mypy src/
```

## Project Structure

```
auto-cdc/
├── src/auto_cdc/          # Main package
│   ├── __init__.py        # Package initialization
│   ├── cdc.py             # Core CDC classes
│   ├── utils.py           # Utility functions
│   └── exceptions.py      # Custom exceptions
├── tests/                 # Test suite
│   ├── conftest.py        # Pytest configuration
│   └── test_cdc.py        # CDC tests
├── docs/                  # Documentation
├── pyproject.toml         # Project configuration
├── README.md              # This file
└── .gitignore             # Git ignore rules
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues, questions, or suggestions, please open an issue on [GitHub Issues](https://github.com/yourusername/auto-cdc/issues).

## Changelog

### Version 0.1.0 (Initial Release)
- Initial implementation of CDC framework
- Support for Delta Lake Change Data Feed
- Automated schema evolution
- Version management and tracking
