# Contributing Guide

Thank you for your interest in contributing to auto-cdc!

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/yourusername/auto-cdc.git`
3. Create a virtual environment: `python -m venv venv`
4. Activate it: `source venv/bin/activate`
5. Install dev dependencies: `pip install -e ".[dev]"`

## Development Workflow

### Before You Start

- Read the [README.md](README.md) to understand the project
- Check existing [issues](https://github.com/yourusername/auto-cdc/issues) to avoid duplicates
- Open an issue to discuss large changes

### Making Changes

1. Create a feature branch: `git checkout -b feature/your-feature-name`
2. Make your changes with meaningful commits
3. Add tests for new functionality
4. Run the test suite: `pytest`
5. Format code: `black src/`
6. Lint code: `ruff check src/`
7. Type check: `mypy src/`

### Submitting a Pull Request

1. Push your branch to your fork
2. Create a [Pull Request](https://github.com/yourusername/auto-cdc/pulls)
3. Describe your changes and reference any related issues
4. Wait for review and address feedback

## Code Style

We follow:
- [PEP 8](https://pep8.org/) for style
- [Google-style docstrings](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings)
- Type hints for function signatures

### Format Code

```bash
black src/ tests/
```

### Run Linters

```bash
ruff check src/ tests/
mypy src/
```

## Testing

- Write tests for all new features
- Maintain test coverage above 80%
- Run tests locally: `pytest`
- Generate coverage report: `pytest --cov`

## Questions?

Open an issue or contact the maintainers. We're here to help!

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
