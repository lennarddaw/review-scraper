"""Review platform scrapers."""

from src.scrapers.review_platforms.trustpilot import TrustpilotScraper
from src.scrapers.review_platforms.sitejabber import SitejabberScraper
from src.scrapers.review_platforms.kununu import KununuScraper
from src.scrapers.review_platforms.reclabox import ReclaboxScraper

__all__ = [
    "TrustpilotScraper",
    "SitejabberScraper",
    "KununuScraper",
    "ReclaboxScraper",
]
