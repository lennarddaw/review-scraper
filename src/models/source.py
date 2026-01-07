"""Source configuration models."""

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class SourceCategory(BaseModel):
    """A category within a source with associated URLs."""

    name: str
    urls: list[str] = Field(default_factory=list)


class Source(BaseModel):
    """Configuration for a review source."""

    name: str
    enabled: bool = True
    scraper: str = Field(..., description="Module path to scraper class")
    rate_limit_rpm: int = Field(default=20, ge=1, le=100)
    requires_browser: bool = False
    categories: list[SourceCategory] = Field(default_factory=list)

    @property
    def all_urls(self) -> list[str]:
        """Get all URLs from all categories."""
        urls = []
        for category in self.categories:
            urls.extend(category.urls)
        return urls

    def get_urls_for_category(self, category_name: str) -> list[str]:
        """Get URLs for a specific category."""
        for category in self.categories:
            if category.name == category_name:
                return category.urls
        return []


class SourceConfig(BaseModel):
    """Configuration container for all sources."""

    sources: dict[str, Source] = Field(default_factory=dict)

    @classmethod
    def from_yaml(cls, path: str | Path) -> "SourceConfig":
        """Load source configuration from a YAML file."""
        path = Path(path)
        
        if not path.exists():
            raise FileNotFoundError(f"Source config file not found: {path}")

        with open(path) as f:
            data = yaml.safe_load(f)

        sources = {}
        for name, config in data.items():
            # Parse categories
            categories = []
            for cat_data in config.get("categories", []):
                categories.append(SourceCategory(
                    name=cat_data.get("name", "default"),
                    urls=cat_data.get("urls", []),
                ))

            sources[name] = Source(
                name=name,
                enabled=config.get("enabled", True),
                scraper=config.get("scraper", f"{name}"),
                rate_limit_rpm=config.get("rate_limit_rpm", 20),
                requires_browser=config.get("requires_browser", False),
                categories=categories,
            )

        return cls(sources=sources)

    def get_enabled_sources(self) -> dict[str, Source]:
        """Get all enabled sources."""
        return {name: source for name, source in self.sources.items() if source.enabled}

    def get_source(self, name: str) -> Source | None:
        """Get a source by name."""
        return self.sources.get(name)

    def get_all_urls(self) -> list[tuple[str, str]]:
        """Get all URLs from all enabled sources with source name."""
        result = []
        for name, source in self.get_enabled_sources().items():
            for url in source.all_urls:
                result.append((name, url))
        return result


def load_sources(config_path: str | Path | None = None) -> SourceConfig:
    """
    Load source configuration.
    
    Args:
        config_path: Path to sources.yaml (default: config/sources.yaml)
    
    Returns:
        SourceConfig object
    """
    if config_path is None:
        config_path = Path(__file__).parent.parent.parent / "config" / "sources.yaml"
    
    return SourceConfig.from_yaml(config_path)