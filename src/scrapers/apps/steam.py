"""Steam game review scraper."""

import json
import re
from datetime import datetime
from urllib.parse import urljoin, urlparse, parse_qs

from bs4 import BeautifulSoup, Tag
from loguru import logger

from src.core.base_scraper import BaseScraper
from src.models.review import Review, ReviewFactory


class SteamScraper(BaseScraper):
    """
    Scraper for Steam game reviews.
    
    Steam provides reviews via both HTML pages and a JSON API.
    We use the API for better reliability and pagination.
    
    API URL: https://store.steampowered.com/appreviews/{app_id}?json=1
    """

    name = "steam"
    base_url = "https://store.steampowered.com"
    rate_limit_rpm = 20
    requires_browser = False

    # Steam reviews API parameters
    API_PARAMS = {
        "json": "1",
        "language": "english",
        "filter": "recent",
        "review_type": "all",
        "purchase_type": "all",
        "num_per_page": "100",
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._review_factory = ReviewFactory()

    async def scrape_reviews(self, url: str, max_reviews: int | None = None) -> list[Review]:
        """
        Scrape reviews from Steam.
        
        Args:
            url: Steam app URL or app ID
            max_reviews: Maximum reviews to collect
            
        Returns:
            List of Review objects
        """
        app_id = self._extract_app_id(url)
        if not app_id:
            logger.error(f"[{self.name}] Could not extract app ID from {url}")
            return []

        try:
            reviews = []
            cursor = "*"
            
            while True:
                # Build API URL
                api_url = f"{self.base_url}/appreviews/{app_id}"
                params = {**self.API_PARAMS, "cursor": cursor}
                
                response = await self.http_client.get(api_url, params=params)
                data = response.json()
                
                if not data.get("success"):
                    logger.warning(f"[{self.name}] API returned unsuccessful response")
                    break

                batch = self._parse_api_reviews(data, url)
                reviews.extend(batch)
                
                logger.debug(f"[{self.name}] Fetched {len(batch)} reviews (total: {len(reviews)})")

                # Check for more pages
                cursor = data.get("cursor")
                if not cursor or not batch:
                    break

                # Check max limit
                if max_reviews and len(reviews) >= max_reviews:
                    reviews = reviews[:max_reviews]
                    break

            return reviews

        except Exception as e:
            logger.error(f"[{self.name}] Error scraping {url}: {e}")
            return []

    def _parse_api_reviews(self, data: dict, source_url: str) -> list[Review]:
        """Parse reviews from Steam API response."""
        reviews = []
        
        for review_data in data.get("reviews", []):
            review = self._parse_review_data(review_data, source_url)
            if review:
                reviews.append(review)

        return reviews

    def _parse_review_data(self, data: dict, source_url: str) -> Review | None:
        """Parse a single review from API data."""
        try:
            # Get review text
            text = data.get("review", "").strip()
            if not text or len(text) < 10:
                return None

            # Get recommendation (thumbs up/down)
            voted_up = data.get("voted_up", True)
            # Convert to 5-star scale (1 for negative, 5 for positive)
            rating = 5.0 if voted_up else 1.0

            # Get timestamp
            date = None
            timestamp = data.get("timestamp_created")
            if timestamp:
                date = datetime.fromtimestamp(timestamp)

            # Get author info
            author_data = data.get("author", {})
            author = author_data.get("steamid")

            # Get helpful count
            helpful_count = data.get("votes_up", 0)

            # Get playtime
            playtime_hours = data.get("author", {}).get("playtime_forever", 0) / 60

            # Add playtime context to text if significant
            if playtime_hours > 1:
                text = f"[{playtime_hours:.1f} hours played]\n\n{text}"

            return self._review_factory.create(
                text=text,
                source=self.name,
                source_url=source_url,
                source_id=data.get("recommendationid"),
                rating=rating,
                date=date,
                author=author,
                helpful_count=helpful_count,
            )

        except Exception as e:
            logger.warning(f"[{self.name}] Error parsing review data: {e}")
            return None

    def parse_review_element(self, element: Tag) -> Review | None:
        """Parse review from HTML element (fallback method)."""
        try:
            text_el = element.select_one("div.content")
            if not text_el:
                return None

            text = text_el.get_text(strip=True)
            if not text:
                return None

            # Get recommendation
            thumb_up = element.select_one("div.thumb img[src*='thumbsUp']")
            rating = 5.0 if thumb_up else 1.0

            return self._review_factory.create(
                text=text,
                source=self.name,
                rating=rating,
            )

        except Exception as e:
            logger.warning(f"[{self.name}] Error parsing HTML review: {e}")
            return None

    async def get_pagination_urls(self, base_url: str, max_pages: int | None = None) -> list[str]:
        """
        Get pagination info.
        
        Note: Steam uses cursor-based pagination via API, so we don't need
        multiple URLs. This method returns just the base URL.
        """
        return [base_url]

    def _extract_app_id(self, url: str) -> str | None:
        """Extract Steam app ID from URL or return as-is if it's already an ID."""
        # Check if it's already an app ID
        if url.isdigit():
            return url

        # Extract from URL
        match = re.search(r"/app/(\d+)", url)
        return match.group(1) if match else None

    @staticmethod
    def build_url(app_id: str) -> str:
        """Build Steam store URL from app ID."""
        return f"https://store.steampowered.com/app/{app_id}/"

    async def get_app_info(self, app_id: str) -> dict | None:
        """Get app information from Steam."""
        try:
            url = f"https://store.steampowered.com/api/appdetails?appids={app_id}"
            response = await self.http_client.get(url)
            data = response.json()
            
            if data.get(app_id, {}).get("success"):
                return data[app_id]["data"]
            return None

        except Exception as e:
            logger.error(f"[{self.name}] Error getting app info: {e}")
            return None