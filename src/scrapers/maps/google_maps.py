"""Google Maps Reviews scraper for local business reviews.

Uses the outscraper library for reliable Google Maps review extraction.
Install: pip install outscraper

Alternative: Can also use SerpApi or direct Places API.

ADAC has 100+ locations across Germany with thousands of reviews.
"""

import asyncio
import os
import re
from datetime import datetime
from typing import AsyncIterator

from loguru import logger

from src.core.base_scraper import BaseScraper
from src.models.review import Review, ReviewFactory

# Try to import outscraper
try:
    from outscraper import ApiClient
    HAS_OUTSCRAPER = True
except ImportError:
    HAS_OUTSCRAPER = False
    logger.warning("outscraper not installed. Run: pip install outscraper")


class GoogleMapsScraper(BaseScraper):
    """
    Scraper for Google Maps business reviews.
    
    Uses outscraper API for reliable extraction.
    Get API key at: https://outscraper.com (free tier available)
    
    Set API key via environment variable: OUTSCRAPER_API_KEY
    Or pass directly to scraper.
    """

    name = "google_maps"
    base_url = "https://www.google.com/maps"
    rate_limit_rpm = 10
    requires_browser = False

    def __init__(self, api_key: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self._review_factory = ReviewFactory()
        self._api_key = api_key or os.getenv("OUTSCRAPER_API_KEY")
        self._client = None
        
        if HAS_OUTSCRAPER and self._api_key:
            self._client = ApiClient(api_key=self._api_key)

    async def scrape_reviews(self, url: str, max_reviews: int | None = None) -> list[Review]:
        """
        Scrape reviews from Google Maps.
        
        Args:
            url: Google Maps URL, Place ID, or search query
            max_reviews: Maximum reviews to collect (default: 100)
            
        Returns:
            List of Review objects
        """
        if not HAS_OUTSCRAPER:
            logger.error(f"[{self.name}] outscraper not installed. Run: pip install outscraper")
            return await self._fallback_scrape(url, max_reviews)
        
        if not self._client:
            logger.error(f"[{self.name}] No API key. Set OUTSCRAPER_API_KEY environment variable")
            return await self._fallback_scrape(url, max_reviews)

        max_reviews = max_reviews or 100
        
        try:
            # Run in thread pool since outscraper is synchronous
            loop = asyncio.get_event_loop()
            
            # Outscraper accepts various formats: URL, Place ID, or search query
            results = await loop.run_in_executor(
                None,
                lambda: self._client.google_maps_reviews(
                    [url],
                    reviews_limit=max_reviews,
                    language='de',
                    region='de',
                )
            )
            
            reviews = self._parse_outscraper_results(results, url)
            logger.info(f"[{self.name}] Collected {len(reviews)} reviews from {url}")
            return reviews
            
        except Exception as e:
            logger.error(f"[{self.name}] Error scraping {url}: {e}")
            return []

    def _parse_outscraper_results(self, results: list, source_url: str) -> list[Review]:
        """Parse results from outscraper API."""
        reviews = []
        
        if not results:
            return reviews
        
        for place_data in results:
            if not isinstance(place_data, dict):
                continue
            
            place_name = place_data.get('name', '')
            place_address = place_data.get('full_address', '')
            
            reviews_data = place_data.get('reviews_data', [])
            if not reviews_data:
                continue
            
            for review_data in reviews_data:
                review = self._parse_review(review_data, place_name, place_address, source_url)
                if review:
                    reviews.append(review)
        
        return reviews

    def _parse_review(self, data: dict, place_name: str, place_address: str, source_url: str) -> Review | None:
        """Parse a single review from API data."""
        try:
            # Review text
            text = data.get('review_text', '').strip()
            if not text or len(text) < 5:
                return None
            
            # Add location context
            if place_name:
                full_text = f"[{place_name}] {text}"
            else:
                full_text = text
            
            # Rating (1-5)
            rating = data.get('review_rating')
            if rating:
                rating = float(rating)
            
            # Date
            date = None
            date_str = data.get('review_datetime_utc') or data.get('review_timestamp')
            if date_str:
                try:
                    if isinstance(date_str, (int, float)):
                        date = datetime.fromtimestamp(date_str)
                    else:
                        date = datetime.fromisoformat(str(date_str).replace('Z', '+00:00'))
                except:
                    pass
            
            # Author
            author = data.get('author_title') or data.get('reviewer_name')
            
            # Helpful count (likes)
            helpful = data.get('review_likes', 0)
            
            # Review ID
            review_id = data.get('review_id')
            
            return self._review_factory.create(
                text=full_text,
                source=self.name,
                source_url=source_url,
                source_id=review_id,
                rating=rating,
                date=date,
                author=author,
                helpful_count=helpful,
                product_name=place_name,
                metadata={
                    "place_name": place_name,
                    "place_address": place_address,
                    "type": "google_maps_review",
                },
            )
            
        except Exception as e:
            logger.debug(f"[{self.name}] Error parsing review: {e}")
            return None

    async def _fallback_scrape(self, url: str, max_reviews: int | None) -> list[Review]:
        """Fallback scraping method without API."""
        logger.warning(f"[{self.name}] Using fallback method - limited results")
        # Without API, we can only get very limited info
        # This is a placeholder for potential future implementation
        return []

    async def scrape_multiple_places(
        self, 
        places: list[str], 
        max_reviews_per_place: int = 50
    ) -> AsyncIterator[Review]:
        """
        Scrape reviews from multiple places.
        
        Args:
            places: List of place URLs, IDs, or search queries
            max_reviews_per_place: Max reviews per location
            
        Yields:
            Review objects
        """
        for place in places:
            try:
                reviews = await self.scrape_reviews(place, max_reviews_per_place)
                for review in reviews:
                    yield review
            except Exception as e:
                logger.error(f"[{self.name}] Error scraping {place}: {e}")
                continue

    def parse_review_element(self, element) -> Review | None:
        """Not used - API handles parsing."""
        return None

    async def get_pagination_urls(self, base_url: str, max_pages: int | None = None) -> list[str]:
        """Return base URL - pagination handled by API."""
        return [base_url]


# ============================================================================
# ADAC STANDORTE - Comprehensive list of ADAC locations in Germany
# ============================================================================

# Search queries for major ADAC location types
ADAC_SEARCH_QUERIES = [
    # Geschäftsstellen (Main offices)
    "ADAC Geschäftsstelle Berlin",
    "ADAC Geschäftsstelle Hamburg",
    "ADAC Geschäftsstelle München",
    "ADAC Geschäftsstelle Köln",
    "ADAC Geschäftsstelle Frankfurt",
    "ADAC Geschäftsstelle Stuttgart",
    "ADAC Geschäftsstelle Düsseldorf",
    "ADAC Geschäftsstelle Hannover",
    "ADAC Geschäftsstelle Leipzig",
    "ADAC Geschäftsstelle Dresden",
    "ADAC Geschäftsstelle Nürnberg",
    "ADAC Geschäftsstelle Bremen",
    "ADAC Geschäftsstelle Essen",
    "ADAC Geschäftsstelle Dortmund",
    
    # Fahrsicherheitszentren (Driving safety centers)
    "ADAC Fahrsicherheitszentrum",
    "ADAC Fahrsicherheitszentrum Linthe",
    "ADAC Fahrsicherheitszentrum Grevenbroich",
    "ADAC Fahrsicherheitszentrum Augsburg",
    "ADAC Fahrsicherheitszentrum Hannover",
    "ADAC Fahrsicherheitszentrum Koblenz",
    
    # Prüfzentren (Testing centers)
    "ADAC Prüfzentrum",
    "ADAC Technikzentrum",
    "ADAC Testzentrum Landsberg",
    
    # Reisebüros
    "ADAC Reisebüro Berlin",
    "ADAC Reisebüro Hamburg",
    "ADAC Reisebüro München",
    
    # General searches by city
    "ADAC Augsburg",
    "ADAC Bonn",
    "ADAC Karlsruhe",
    "ADAC Mannheim",
    "ADAC Wiesbaden",
    "ADAC Münster",
    "ADAC Aachen",
    "ADAC Bielefeld",
    "ADAC Braunschweig",
    "ADAC Kiel",
    "ADAC Lübeck",
    "ADAC Magdeburg",
    "ADAC Erfurt",
    "ADAC Rostock",
    "ADAC Potsdam",
    "ADAC Saarbrücken",
    "ADAC Freiburg",
    "ADAC Mainz",
    "ADAC Regensburg",
    "ADAC Würzburg",
    "ADAC Ulm",
    "ADAC Ingolstadt",
    "ADAC Kassel",
    "ADAC Oldenburg",
    "ADAC Osnabrück",
    "ADAC Paderborn",
    "ADAC Göttingen",
    "ADAC Wolfsburg",
    "ADAC Heilbronn",
    "ADAC Pforzheim",
    "ADAC Reutlingen",
    "ADAC Tübingen",
]

# Specific Google Maps Place IDs for major ADAC locations (if known)
# Format: "place_id:ChIJ..." or direct Google Maps URLs
ADAC_PLACE_IDS = [
    # These would need to be looked up - examples:
    # "ChIJxxxx" - ADAC Zentrale München
    # "ChIJyyyy" - ADAC Berlin
]

# Direct Google Maps URLs
ADAC_MAPS_URLS = [
    # Example URLs - need actual URLs
    # "https://www.google.com/maps/place/ADAC+Zentrale/@48.1351253,11.5819806,17z",
]


def get_adac_locations() -> list[str]:
    """Get list of ADAC locations to scrape."""
    locations = []
    
    # Add search queries
    locations.extend(ADAC_SEARCH_QUERIES)
    
    # Add place IDs if available
    locations.extend(ADAC_PLACE_IDS)
    
    # Add direct URLs if available
    locations.extend(ADAC_MAPS_URLS)
    
    return locations
