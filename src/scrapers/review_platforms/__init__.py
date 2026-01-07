"""Review platform scrapers."""

from src.scrapers.review_platforms.trustpilot import TrustpilotScraper
from src.scrapers.review_platforms.sitejabber import SitejabberScraper

__all__ = [
    "TrustpilotScraper",
    "SitejabberScraper",
]