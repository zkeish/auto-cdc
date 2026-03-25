# Development Guide

## Project Structure

```
auto-cdc/
├── src/auto_cdc/              # Main package
│   ├── __init__.py            # Exports public API
│   ├── cdc.py                 # Core CDC classes (CDC, HelperFunctions)
│   ├── utils.py               # Utility functions (Utils class)
│   └── exceptions.py          # Custom exception classes
├── tests/                     # Test suite
│   ├── __init__.py
│   ├── conftest.py            # Pytest configuration and fixtures
│   └── test_cdc.py            # CDC tests
├── docs/                      # Documentation (future)
├── pyproject.toml             # PEP 518 project configuration
├── README.md                  # User-facing documentation
├── CONTRIBUTING.md            # Contribution guidelines
├── LICENSE                    # MIT License
└── .gitignore                 # Git ignore rules
```

## Key Modules

### `cdc.py`

Contains the main classes:

- **CDC**: Public API for CDC operations
  - `write_to_cdc_feed()`: Main entry point for writing changes
  - `delete_cdc_feed()`: Clean up a CDC feed
  - `cdc_tracker_table_name()`: Utility for table naming

- **HelperFunctions**: Internal helper class
  - Schema management
  - Merge operations
  - Version tracking
  - Data vacuum

### `utils.py`

Utility functions:

- **Utils.format_file_datetime()**: Format datetime to filename
- **Utils.push_to_volume()**: Write dataframe to volume

### `exceptions.py`

Custom exception hierarchy:

- **CDCException**: Base exception
  - **CDCVersionError**: Version-related issues
  - **CDCSchemaError**: Schema-related issues
  - **CDCFileError**: File operation issues

## Working with Databricks

The package is designed for Databricks environments using:

- **Delta Lake**: Underlying table format
- **Change Data Feed**: For tracking changes
- **Volumes**: For output storage
- **dbutils**: For file operations
- **spark**: PySpark context

### Local Development

For local development without Databricks:

```python
# The code gracefully handles missing Databricks dependencies
import auto_cdc  # Works even without Databricks
```

## Configuration

### `pyproject.toml`

Defines project metadata and dependencies:

- **dependencies**: Core runtime dependencies
- **optional-dependencies[dev]**: Development tools
- **optional-dependencies[test]**: Testing tools
- **tool.pytest**: Pytest configuration
- **tool.black**: Code formatting rules
- **tool.ruff**: Linting rules
- **tool.mypy**: Type checking rules

## Installation Modes

### User Installation

```bash
pip install auto-cdc
```

### Development Installation

```bash
pip install -e ".[dev]"  # Installs with dev tools
```

### Test Installation

```bash
pip install -e ".[test]"  # Installs with test tools
```

## Running Tests

### All Tests

```bash
pytest
```

### With Coverage

```bash
pytest --cov=src/auto_cdc
```

### Specific Test File

```bash
pytest tests/test_cdc.py
```

### Specific Test

```bash
pytest tests/test_cdc.py::TestCDC::test_cdc_tracker_table_name
```

### With Verbose Output

```bash
pytest -v
```

## Type Checking

```bash
mypy src/
```

Configuration in `pyproject.toml` under `[tool.mypy]`.

## Code Formatting

Format all Python files:

```bash
black src/ tests/
```

Check formatting without changes:

```bash
black --check src/ tests/
```

## Linting

Run all lints:

```bash
ruff check src/ tests/
```

Fix auto-fixable issues:

```bash
ruff check --fix src/ tests/
```

## Building the Package

### Build Distribution Files

```bash
pip install build
python -m build
```

This creates:
- `dist/auto_cdc-*.whl` (wheel distribution)
- `dist/auto_cdc-*.tar.gz` (source distribution)

### Test Installation from Build

```bash
pip install dist/auto_cdc-*.whl
```

## Documentation

Documentation files are in the `docs/` directory.

Building docs (once Sphinx is configured):

```bash
cd docs
make html
```

View in browser: `docs/_build/html/index.html`

## Releasing

1. Update version in `src/auto_cdc/__init__.py`
2. Update `pyproject.toml` version
3. Update `CHANGELOG.md` (create if needed)
4. Tag release: `git tag v0.1.0`
5. Build: `python -m build`
6. Upload to PyPI: `twine upload dist/*`

## Troubleshooting

### Import Issues

If you can't import `auto_cdc`:

```bash
pip install -e .  # Reinstall in editable mode
```

### Test Failures

Ensure you're in the right environment:

```bash
which python  # Should point to venv
pytest -v     # Verbose output
```

### Databricks Dependencies

Some features require Databricks dependencies. Install the full environment:

```bash
pip install -e ".[dev]"
```

## Performance Considerations

- **Vacuum Operations**: Call `vacuum_data()` periodically to clean up old versions
- **Large Merges**: Consider partitioning when merging large tables
- **Schema Evolution**: Monitor schema changes for performance impact
- **Version History**: Older versions are automatically cleaned based on `vacuum_days` parameter
