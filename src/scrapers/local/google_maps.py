"""Google Maps/Places review scraper for location-based businesses."""

import re
import json
import asyncio
from datetime import datetime
from urllib.parse import quote, urljoin

from bs4 import BeautifulSoup, Tag
from loguru import logger

from src.core.base_scraper import BaseScraper
from src.models.review import Review, ReviewFactory

# Try to import outscraper (preferred method)
try:
    from outscraper import ApiClient as OutscraperClient
    HAS_OUTSCRAPER = True
except ImportError:
    HAS_OUTSCRAPER = False


class GoogleMapsScraper(BaseScraper):
    """
    Scraper for Google Maps/Places reviews.
    
    Supports multiple methods:
    1. Outscraper API (requires API key, most reliable)
    2. SerpAPI (requires API key)
    3. Direct scraping (limited, fallback)
    
    URL formats:
    - Place ID: ChIJ...
    - Search query: "ADAC Geschäftsstelle München"
    - Maps URL: https://www.google.com/maps/place/...
    """

    name = "google_maps"
    base_url = "https://www.google.com/maps"
    rate_limit_rpm = 10
    requires_browser = False

    def __init__(self, api_key: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self._review_factory = ReviewFactory()
        self._api_key = api_key
        
        # Check for API key in environment
        if not self._api_key:
            import os
            self._api_key = os.environ.get("OUTSCRAPER_API_KEY") or os.environ.get("GOOGLE_MAPS_API_KEY")

    async def scrape_reviews(self, url: str, max_reviews: int | None = None) -> list[Review]:
        """
        Scrape reviews from Google Maps.
        
        Args:
            url: Place ID, search query, or Maps URL
            max_reviews: Maximum reviews to collect
            
        Returns:
            List of Review objects
        """
        max_reviews = max_reviews or 100
        
        # Try different methods
        if HAS_OUTSCRAPER and self._api_key:
            return await self._scrape_with_outscraper(url, max_reviews)
        else:
            # Fallback to direct scraping (limited)
            return await self._scrape_direct(url, max_reviews)

    async def _scrape_with_outscraper(self, query: str, max_reviews: int) -> list[Review]:
        """Scrape using Outscraper API."""
        try:
            loop = asyncio.get_event_loop()
            client = OutscraperClient(api_key=self._api_key)
            
            # Run synchronous API call in executor
            results = await loop.run_in_executor(
                None,
                lambda: client.google_maps_reviews(
                    query,
                    reviews_limit=max_reviews,
                    language='de',
                    region='de',
                )
            )
            
            reviews = []
            if results and len(results) > 0:
                place_data = results[0]
                place_name = place_data.get('name', '')
                place_address = place_data.get('address', '')
                
                for review_data in place_data.get('reviews_data', []):
                    review = self._parse_outscraper_review(review_data, place_name, place_address)
                    if review:
                        reviews.append(review)
            
            logger.info(f"[{self.name}] Collected {len(reviews)} reviews via Outscraper")
            return reviews
            
        except Exception as e:
            logger.error(f"[{self.name}] Outscraper API error: {e}")
            return []

    def _parse_outscraper_review(self, data: dict, place_name: str, place_address: str) -> Review | None:
        """Parse a review from Outscraper API response."""
        try:
            text = data.get('review_text', '').strip()
            if not text or len(text) < 5:
                return None
            
            # Add location context
            if place_name:
                text = f"[{place_name}] {text}"
            
            # Rating
            rating = data.get('review_rating')
            
            # Date
            date = None
            date_str = data.get('review_datetime_utc')
            if date_str:
                try:
                    date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                except:
                    pass
            
            # Author
            author = data.get('author_title')
            
            # Review ID
            review_id = data.get('review_id')
            
            # Likes
            likes = data.get('review_likes', 0)
            
            return self._review_factory.create(
                text=text,
                source=self.name,
                source_id=review_id,
                rating=float(rating) if rating else None,
                date=date,
                author=author,
                helpful_count=likes,
                metadata={
                    "place_name": place_name,
                    "place_address": place_address,
                    "type": "google_maps_review",
                },
            )
            
        except Exception as e:
            logger.warning(f"[{self.name}] Error parsing review: {e}")
            return None

    async def _scrape_direct(self, url: str, max_reviews: int) -> list[Review]:
        """
        Direct scraping fallback (limited functionality).
        
        Note: Google Maps is heavily JavaScript-based, so direct scraping
        has limited capabilities. This method tries to extract what's available
        from the initial HTML response.
        """
        logger.warning(f"[{self.name}] Using direct scraping (limited). Consider using Outscraper API.")
        
        try:
            # Build search URL if needed
            if not url.startswith("http"):
                url = f"https://www.google.com/maps/search/{quote(url)}"
            
            headers = {
                "Accept-Language": "de-DE,de;q=0.9",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            }
            
            html = await self.http_client.get_text(url, headers=headers)
            
            # Try to extract embedded JSON data
            reviews = self._extract_reviews_from_html(html, url)
            
            if not reviews:
                logger.info(f"[{self.name}] No reviews found via direct scraping. Google Maps requires JavaScript.")
            
            return reviews[:max_reviews]
            
        except Exception as e:
            logger.error(f"[{self.name}] Direct scraping error: {e}")
            return []

    def _extract_reviews_from_html(self, html: str, source_url: str) -> list[Review]:
        """Try to extract reviews from Google Maps HTML/JSON."""
        reviews = []
        
        # Google embeds data in script tags as JSON
        # Look for review data patterns
        patterns = [
            r'\["([^"]{20,500})"\s*,\s*(\d)\s*,',  # Review text with rating
            r'"reviewText"\s*:\s*"([^"]{20,500})"',
            r'"text"\s*:\s*"([^"]{20,500})".*?"rating"\s*:\s*(\d)',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, html)
            for match in matches:
                if isinstance(match, tuple):
                    text = match[0] if len(match) > 0 else ""
                    rating = float(match[1]) if len(match) > 1 else None
                else:
                    text = match
                    rating = None
                
                # Clean up escaped characters
                text = text.encode().decode('unicode_escape')
                
                if text and len(text) >= 20:
                    review = self._review_factory.create(
                        text=text,
                        source=self.name,
                        source_url=source_url,
                        rating=rating,
                    )
                    reviews.append(review)
        
        return reviews

    async def get_pagination_urls(self, base_url: str, max_pages: int | None = None) -> list[str]:
        """Return base URL - pagination handled internally."""
        return [base_url]

    def parse_review_element(self, element: Tag) -> Review | None:
        """Not used for Google Maps."""
        return None

    @staticmethod
    def build_search_url(query: str) -> str:
        """Build Google Maps search URL."""
        return f"https://www.google.com/maps/search/{quote(query)}"


# ADAC Standorte für Google Maps Scraping
ADAC_LOCATIONS = {
    # Geschäftsstellen - Major cities
    "geschaeftsstellen": [
        "ADAC Geschäftsstelle München",
        "ADAC Geschäftsstelle Berlin",
        "ADAC Geschäftsstelle Hamburg",
        "ADAC Geschäftsstelle Köln",
        "ADAC Geschäftsstelle Frankfurt",
        "ADAC Geschäftsstelle Stuttgart",
        "ADAC Geschäftsstelle Düsseldorf",
        "ADAC Geschäftsstelle Dortmund",
        "ADAC Geschäftsstelle Essen",
        "ADAC Geschäftsstelle Leipzig",
        "ADAC Geschäftsstelle Dresden",
        "ADAC Geschäftsstelle Hannover",
        "ADAC Geschäftsstelle Nürnberg",
        "ADAC Geschäftsstelle Bremen",
        "ADAC Geschäftsstelle Augsburg",
        "ADAC Geschäftsstelle Mannheim",
        "ADAC Geschäftsstelle Karlsruhe",
        "ADAC Geschäftsstelle Wiesbaden",
        "ADAC Geschäftsstelle Münster",
        "ADAC Geschäftsstelle Bonn",
    ],
    
    # Fahrtechnik Zentren (Fahrsicherheitszentren)
    "fahrtechnik": [
        "ADAC Fahrsicherheitszentrum Grevenbroich",
        "ADAC Fahrsicherheitszentrum Linthe",
        "ADAC Fahrsicherheitszentrum Augsburg",
        "ADAC Fahrsicherheitszentrum Kempten",
        "ADAC Fahrsicherheitszentrum Hannover",
        "ADAC Fahrsicherheitszentrum Hockenheim",
        "ADAC Fahrsicherheitszentrum Nürburgring",
        "ADAC Fahrtechnik Zentrum",
    ],
    
    # Prüfzentren
    "pruefzentren": [
        "ADAC Prüfzentrum",
        "ADAC TÜV Prüfstelle",
    ],
    
    # Reisebüros
    "reisebueros": [
        "ADAC Reisebüro München",
        "ADAC Reisebüro Berlin",
        "ADAC Reisebüro Hamburg",
        "ADAC Reisebüro Frankfurt",
        "ADAC Reisebüro Köln",
    ],
}


def get_all_adac_search_queries() -> list[str]:
    """Get all ADAC location search queries."""
    queries = []
    for category, locations in ADAC_LOCATIONS.items():
        queries.extend(locations)
    return queries
