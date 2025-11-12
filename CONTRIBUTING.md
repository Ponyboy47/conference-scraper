# Contributing

Thank you for your interest in contributing to conference-scraper! This document provides guidelines and setup instructions for contributors.

## Development Setup

### Prerequisites

- Python 3.12 or later
- [uv](https://github.com/astral-sh/uv) for dependency management

### Installation

1. Clone the repository:

   git clone https://github.com/yourusername/conference-scraper.git
   cd conference-scraper

1. Install dependencies:

   uv sync

### Pre-commit Hooks

We use [pre-commit](https://pre-commit.com/) to run linting and formatting checks before commits. To install pre-commit hooks:

uvx pre-commit install

This will install pre-commit using uvx (which runs it without installing it globally) and set up the git hooks.

### Running Pre-commit Checks

To run all pre-commit checks manually:

uvx pre-commit run --all-files

Or to run on staged files only:

uvx pre-commit run

### Code Quality Tools

This project uses:

- **Ruff**: For linting and formatting Python code
- **MdFormat**: For linting and formatting Markdown files
- **Pre-commit**: To run checks automatically before commits

### Development Workflow

1. Create a new branch for your feature/fix:

   git checkout -b feature/your-feature-name

1. Make your changes, ensuring you follow the code style (ruff+mdformat will help with this)

1. Run pre-commit checks:

   uvx pre-commit run --all-files

1. Commit your changes:

   git add .
   git commit -m "Your descriptive commit message"

1. Push and create a pull request

### Running the Scraper

To run the conference scraper:

uv run main.py scrape

## Code Style

- Follow PEP 8 style guidelines
- Use Ruff & MdFormat for automatic formatting and linting
- Pre-commit hooks will enforce these standards

## Testing

Currently, this project does not have automated tests. When contributing new features, please ensure:

- Your code follows the existing patterns
- You test your changes manually
- Pre-commit checks pass

## Questions?

If you have questions about contributing, feel free to open an issue or start a discussion in the repository.
