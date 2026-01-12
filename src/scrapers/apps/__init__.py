"""App store and software platform scrapers."""

from src.scrapers.apps.steam import SteamScraper
from src.scrapers.apps.google_play import GooglePlayScraper

__all__ = [
    "SteamScraper",
    "GooglePlayScraper",
]
