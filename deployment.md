## Deploying to PyPI

### 1. Build distributions

From the repository root where `pyproject.toml` is located:

```bash
python3 -m pip install --upgrade build
python3 -m build
```

This creates package files in `dist/`, typically a wheel and a source archive.

### 2. Upload to PyPI

Install `twine` and upload the built distributions:

```bash
python3 -m pip install --upgrade twine
python3 -m twine upload dist/*
```

Enter your PyPI API token when prompted. Include the full token value, including the `pypi-` prefix.

### 3. Optional: verify on TestPyPI first

To test package upload before publishing to the live index:

```bash
python3 -m twine upload --repository testpypi dist/*
```

Then install from TestPyPI:

```bash
python3 -m pip install --index-url https://test.pypi.org/simple/ --no-deps auto-cdc
```

### Notes

- Build from the project root with `python3 -m build`.
- Upload `dist/*` with Twine after building.
- Use TestPyPI to verify releases before uploading to the main PyPI index.
- Confirm `pyproject.toml` metadata and version are correct before upload.
