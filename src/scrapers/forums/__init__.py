"""Forum and Q&A platform scrapers."""

from src.scrapers.forums.gutefrage import GutefrageScraper
from src.scrapers.forums.motor_talk import MotorTalkScraper
from src.scrapers.forums.reddit import RedditScraper

__all__ = [
    "GutefrageScraper",
    "MotorTalkScraper",
    "RedditScraper",
]
