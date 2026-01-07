"""Scraper plugin registry for dynamic scraper loading."""

from importlib import import_module
from typing import Type

from loguru import logger

from src.core.base_scraper import BaseScraper


class ScraperRegistry:
    """
    Registry for scraper plugins.
    
    Allows dynamic loading of scrapers by name and maintains
    a registry of all available scrapers.
    """

    _scrapers: dict[str, Type[BaseScraper]] = {}

    @classmethod
    def register(cls, name: str, scraper_class: Type[BaseScraper]) -> None:
        """
        Register a scraper class.

        Args:
            name: Unique scraper name
            scraper_class: Scraper class to register
        """
        cls._scrapers[name] = scraper_class
        logger.debug(f"Registered scraper: {name}")

    @classmethod
    def get(cls, name: str) -> Type[BaseScraper] | None:
        """
        Get a scraper class by name.

        Args:
            name: Scraper name

        Returns:
            Scraper class or None
        """
        return cls._scrapers.get(name)

    @classmethod
    def list_scrapers(cls) -> list[str]:
        """Get list of all registered scraper names."""
        return list(cls._scrapers.keys())

    @classmethod
    def load_from_module(cls, module_path: str) -> Type[BaseScraper] | None:
        """
        Dynamically load a scraper from a module path.

        Args:
            module_path: Module path like 'review_platforms.trustpilot'

        Returns:
            Scraper class or None
        """
        try:
            full_path = f"src.scrapers.{module_path}"
            module = import_module(full_path)
            
            # Look for a class that ends with 'Scraper'
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if (
                    isinstance(attr, type)
                    and issubclass(attr, BaseScraper)
                    and attr is not BaseScraper
                    and not attr_name.startswith("_")
                ):
                    cls.register(module_path, attr)
                    return attr

            logger.warning(f"No scraper class found in {full_path}")
            return None

        except ImportError as e:
            logger.error(f"Failed to import scraper module {module_path}: {e}")
            return None


def get_scraper(name: str) -> Type[BaseScraper] | None:
    """
    Get a scraper by name, loading if necessary.

    Args:
        name: Scraper name or module path

    Returns:
        Scraper class or None
    """
    # First check registry
    scraper = ScraperRegistry.get(name)
    if scraper:
        return scraper

    # Try to load from module
    return ScraperRegistry.load_from_module(name)


# Register built-in scrapers
def _register_builtin_scrapers():
    """Register all built-in scrapers."""
    builtin_scrapers = [
        "review_platforms.trustpilot",
        "review_platforms.sitejabber",
        "entertainment.imdb",
        "apps.steam",
    ]

    for scraper_path in builtin_scrapers:
        try:
            ScraperRegistry.load_from_module(scraper_path)
        except Exception as e:
            logger.debug(f"Could not load {scraper_path}: {e}")


# Auto-register on import (lazy loading)
# _register_builtin_scrapers()