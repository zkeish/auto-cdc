# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2024-03-24

### Initial Release

**Added**
- Core CDC framework for Databricks Delta Lake
- `CDC` class with main public API:
  - `write_to_cdc_feed()`: Write changes to CDC feed with automatic tracking
  - `delete_cdc_feed()`: Remove CDC feed and associated tables
  - `cdc_tracker_table_name()`: Generate table names from paths
- `HelperFunctions` class for internal CDC operations:
  - Schema evolution and change detection
  - Version management with timestamp tracking
  - Merge operations with configurable conditions
  - Delta table vacuum and retention
  - Change feed integration
- `Utils` class for utility functions:
  - File datetime formatting
  - Volume write operations
- Custom exception hierarchy:
  - `CDCException`: Base exception class
  - `CDCVersionError`: Version-related errors
  - `CDCSchemaError`: Schema evolution errors
  - `CDCFileError`: File operation errors
- Comprehensive package structure (src-layout)
- Full test suite with pytest
- Documentation (README, DEVELOPMENT, CONTRIBUTING)
- Build system with `pyproject.toml`
- MIT License

## Versioning

Starting at v0.1.0 for initial development. Will follow semantic versioning once API stabilizes.
