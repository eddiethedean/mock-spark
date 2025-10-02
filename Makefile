.PHONY: help install install-dev test test-cov lint format type-check clean build publish

help: ## Show this help message
	@echo "Mock Spark Package Management"
	@echo "============================="
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install the package in development mode
	pip install -e .

install-dev: ## Install with development dependencies
	pip install -e ".[dev]"

test: ## Run tests
	python test_basic.py

test-cov: ## Run tests with coverage
	pytest --cov=mock_spark --cov-report=html --cov-report=term

lint: ## Run linting
	black --check mock_spark/
	isort --check-only mock_spark/

format: ## Format code
	black mock_spark/
	isort mock_spark/

type-check: ## Run mypy type checking
	mypy mock_spark/

clean: ## Clean build artifacts
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf htmlcov/

build: ## Build the package
	python -m build

publish: ## Publish to PyPI (requires authentication)
	twine upload dist/*

all: clean format type-check test build ## Run all checks and build
