# auto-cdc

**Lightweight Python helper for Change Data Capture on Delta Lake**

`auto-cdc` helps teams build and maintain Delta Lake change data capture workflows from PySpark. It turns Spark DataFrames into managed CDC feeds by automating table creation, schema evolution, merge-based upserts, version tracking, and retention cleanup. The package is designed to reduce the boilerplate needed for incremental CDC writes and make Delta Lake CDC easier to operate.

## Features

- **CDC writes to Delta Lake**: Write Spark DataFrames into Delta-based CDC feeds
- **Schema evolution handling**: Detects schema changes and updates the target table as needed
- **Merge-based upserts**: Updates existing rows and inserts new rows using configurable key matching
- **Version tracking**: Stores CDC version and source timestamp history in helper tables
- **Retention cleanup**: Supports vacuuming old Delta versions based on configured retention days
- **Databricks-aware**: Enables Delta Change Data Feed when running inside Databricks

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

### Version 0.1.3 (Stability)
- Changing internal references