"""Local/Regional business review scrapers - FREE & UNLIMITED."""

from src.scrapers.local.golocal import GoLocalScraper
from src.scrapers.local.gelbe_seiten import GelbeSeitenScraper
from src.scrapers.local.yelp_de import YelpDeScraper
from src.scrapers.local.scraper_11880 import Scraper11880
from src.scrapers.local.kennstdueinen import KennstDuEinenScraper
from src.scrapers.local.werkenntdenbesten import WerKenntDenBestenScraper

__all__ = [
    "GoLocalScraper",
    "GelbeSeitenScraper",
    "YelpDeScraper",
    "Scraper11880",
    "KennstDuEinenScraper",
    "WerKenntDenBestenScraper",
]
