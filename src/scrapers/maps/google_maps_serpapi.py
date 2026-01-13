"""Google Maps Reviews scraper using SerpApi.

SerpApi has a generous free tier (100 searches/month).
Get API key at: https://serpapi.com

This is an alternative to outscraper for Google Maps reviews.
"""

import asyncio
import os
import re
from datetime import datetime
from typing import AsyncIterator

from loguru import logger

from src.core.base_scraper import BaseScraper
from src.models.review import Review, ReviewFactory

# Try to import serpapi
try:
    from serpapi import GoogleSearch
    HAS_SERPAPI = True
except ImportError:
    HAS_SERPAPI = False
    logger.warning("serpapi not installed. Run: pip install google-search-results")


class GoogleMapsSerpApiScraper(BaseScraper):
    """
    Scraper for Google Maps reviews using SerpApi.
    
    SerpApi provides reliable Google Maps data extraction.
    Free tier: 100 searches/month
    
    Get API key at: https://serpapi.com
    Set via: SERPAPI_API_KEY environment variable
    """

    name = "google_maps_serp"
    base_url = "https://www.google.com/maps"
    rate_limit_rpm = 10
    requires_browser = False

    def __init__(self, api_key: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self._review_factory = ReviewFactory()
        self._api_key = api_key or os.getenv("SERPAPI_API_KEY")

    async def scrape_reviews(self, url: str, max_reviews: int | None = None) -> list[Review]:
        """
        Scrape reviews from Google Maps using SerpApi.
        
        Args:
            url: Search query (e.g., "ADAC Geschäftsstelle München")
                 or data_id from Google Maps
            max_reviews: Maximum reviews to collect
            
        Returns:
            List of Review objects
        """
        if not HAS_SERPAPI:
            logger.error(f"[{self.name}] serpapi not installed. Run: pip install google-search-results")
            return []
        
        if not self._api_key:
            logger.error(f"[{self.name}] No API key. Set SERPAPI_API_KEY environment variable")
            logger.info("Get free API key at: https://serpapi.com")
            return []

        max_reviews = max_reviews or 50
        
        try:
            # Run in thread pool
            loop = asyncio.get_event_loop()
            
            # First, search for the place to get data_id
            place_data = await loop.run_in_executor(
                None,
                lambda: self._search_place(url)
            )
            
            if not place_data:
                logger.warning(f"[{self.name}] No place found for: {url}")
                return []
            
            # Then get reviews for the place
            reviews = await loop.run_in_executor(
                None,
                lambda: self._get_reviews(place_data, max_reviews)
            )
            
            logger.info(f"[{self.name}] Collected {len(reviews)} reviews from {url}")
            return reviews
            
        except Exception as e:
            logger.error(f"[{self.name}] Error scraping {url}: {e}")
            return []

    def _search_place(self, query: str) -> dict | None:
        """Search for a place on Google Maps."""
        try:
            params = {
                "engine": "google_maps",
                "q": query,
                "type": "search",
                "api_key": self._api_key,
                "hl": "de",
                "gl": "de",
            }
            
            search = GoogleSearch(params)
            results = search.get_dict()
            
            # Get first local result
            local_results = results.get("local_results", [])
            if local_results:
                return local_results[0]
            
            # Or from place_results
            place_results = results.get("place_results", {})
            if place_results:
                return place_results
            
            return None
            
        except Exception as e:
            logger.error(f"[{self.name}] Search error: {e}")
            return None

    def _get_reviews(self, place_data: dict, max_reviews: int) -> list[Review]:
        """Get reviews for a place."""
        reviews = []
        
        place_name = place_data.get("title", "")
        place_address = place_data.get("address", "")
        data_id = place_data.get("data_id") or place_data.get("place_id")
        
        if not data_id:
            # Try to extract from URL
            place_url = place_data.get("place_id_search")
            if place_url:
                match = re.search(r"0x[a-f0-9]+:0x[a-f0-9]+", place_url)
                if match:
                    data_id = match.group(0)
        
        if not data_id:
            logger.warning(f"[{self.name}] No data_id found for {place_name}")
            # Return any inline reviews from the search
            inline_reviews = place_data.get("reviews", [])
            for review_data in inline_reviews[:max_reviews]:
                review = self._parse_review(review_data, place_name, place_address)
                if review:
                    reviews.append(review)
            return reviews
        
        try:
            # Get detailed reviews
            params = {
                "engine": "google_maps_reviews",
                "data_id": data_id,
                "api_key": self._api_key,
                "hl": "de",
                "sort_by": "newestFirst",
            }
            
            # Paginate through reviews
            next_page_token = None
            while len(reviews) < max_reviews:
                if next_page_token:
                    params["next_page_token"] = next_page_token
                
                search = GoogleSearch(params)
                results = search.get_dict()
                
                reviews_data = results.get("reviews", [])
                if not reviews_data:
                    break
                
                for review_data in reviews_data:
                    if len(reviews) >= max_reviews:
                        break
                    review = self._parse_review(review_data, place_name, place_address)
                    if review:
                        reviews.append(review)
                
                # Check for more pages
                next_page_token = results.get("serpapi_pagination", {}).get("next_page_token")
                if not next_page_token:
                    break
            
        except Exception as e:
            logger.error(f"[{self.name}] Error getting reviews: {e}")
        
        return reviews

    def _parse_review(self, data: dict, place_name: str, place_address: str) -> Review | None:
        """Parse a single review."""
        try:
            # Review text
            text = data.get("snippet", "") or data.get("text", "")
            if not text or len(text.strip()) < 5:
                return None
            
            text = text.strip()
            
            # Add location context
            if place_name:
                full_text = f"[{place_name}] {text}"
            else:
                full_text = text
            
            # Rating
            rating = data.get("rating")
            if rating:
                rating = float(rating)
            
            # Date
            date = None
            date_str = data.get("iso_date") or data.get("date")
            if date_str:
                try:
                    date = datetime.fromisoformat(str(date_str).replace('Z', '+00:00'))
                except:
                    # Try to parse relative date
                    date = self._parse_relative_date(date_str)
            
            # Author
            author = data.get("user", {}).get("name") if isinstance(data.get("user"), dict) else data.get("user")
            
            # Likes
            likes = data.get("likes", 0)
            
            return self._review_factory.create(
                text=full_text,
                source=self.name,
                rating=rating,
                date=date,
                author=author,
                helpful_count=likes,
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

    def _parse_relative_date(self, date_str: str) -> datetime | None:
        """Parse relative date strings like 'vor 2 Wochen'."""
        if not date_str:
            return None
        
        from datetime import timedelta
        now = datetime.now()
        
        date_lower = date_str.lower()
        
        # German patterns
        patterns = [
            (r"vor (\d+) minute", lambda m: now - timedelta(minutes=int(m.group(1)))),
            (r"vor (\d+) stunde", lambda m: now - timedelta(hours=int(m.group(1)))),
            (r"vor (\d+) tag", lambda m: now - timedelta(days=int(m.group(1)))),
            (r"vor (\d+) woche", lambda m: now - timedelta(weeks=int(m.group(1)))),
            (r"vor (\d+) monat", lambda m: now - timedelta(days=int(m.group(1)) * 30)),
            (r"vor (\d+) jahr", lambda m: now - timedelta(days=int(m.group(1)) * 365)),
            # English patterns
            (r"(\d+) minute.* ago", lambda m: now - timedelta(minutes=int(m.group(1)))),
            (r"(\d+) hour.* ago", lambda m: now - timedelta(hours=int(m.group(1)))),
            (r"(\d+) day.* ago", lambda m: now - timedelta(days=int(m.group(1)))),
            (r"(\d+) week.* ago", lambda m: now - timedelta(weeks=int(m.group(1)))),
            (r"(\d+) month.* ago", lambda m: now - timedelta(days=int(m.group(1)) * 30)),
            (r"(\d+) year.* ago", lambda m: now - timedelta(days=int(m.group(1)) * 365)),
        ]
        
        for pattern, calc_func in patterns:
            match = re.search(pattern, date_lower)
            if match:
                return calc_func(match)
        
        return None

    def parse_review_element(self, element) -> Review | None:
        """Not used - API handles parsing."""
        return None

    async def get_pagination_urls(self, base_url: str, max_pages: int | None = None) -> list[str]:
        """Return base URL - pagination handled by API."""
        return [base_url]
