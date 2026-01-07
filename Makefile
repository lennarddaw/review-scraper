.PHONY: install dev test lint format clean scrape help

# Default Python
PYTHON := python

help:
	@echo "Review Scraper - Available commands:"
	@echo ""
	@echo "  make install    - Install package and dependencies"
	@echo "  make dev        - Install with development dependencies"
	@echo "  make test       - Run tests"
	@echo "  make lint       - Run linter"
	@echo "  make format     - Format code"
	@echo "  make clean      - Clean build artifacts"
	@echo "  make scrape     - Run example scrape"
	@echo ""

install:
	$(PYTHON) -m pip install -e .

dev:
	$(PYTHON) -m pip install -e ".[dev]"
	playwright install chromium

test:
	$(PYTHON) -m pytest tests/ -v

lint:
	$(PYTHON) -m ruff check src/ cli/

format:
	$(PYTHON) -m ruff format src/ cli/

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

scrape:
	$(PYTHON) -m cli.main scrape trustpilot --url https://www.trustpilot.com/review/amazon.com --max 100

dirs:
	mkdir -p data/exports data/raw data/checkpoints logs