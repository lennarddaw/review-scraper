"""Site-specific scrapers for different review platforms."""

from src.scrapers.registry import ScraperRegistry, get_scraper

__all__ = [
    "ScraperRegistry",
    "get_scraper",
]