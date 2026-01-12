"""Google Play Store app review scraper."""

import re
import json
from datetime import datetime
from urllib.parse import quote

from bs4 import BeautifulSoup, Tag
from loguru import logger

from src.core.base_scraper import BaseScraper
from src.models.review import Review, ReviewFactory


class GooglePlayScraper(BaseScraper):
    """
    Scraper for Google Play Store app reviews.
    
    Google Play uses dynamic loading, but we can use their internal API
    to fetch reviews in batches.
    
    URL format: https://play.google.com/store/apps/details?id={package_id}
    """

    name = "google_play"
    base_url = "https://play.google.com"
    rate_limit_rpm = 15
    requires_browser = False

    # Google Play internal API endpoint for reviews
    REVIEW_API = "https://play.google.com/_/PlayStoreUi/data/batchexecute"
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._review_factory = ReviewFactory()

    async def scrape_reviews(self, url: str, max_reviews: int | None = None) -> list[Review]:
        """
        Scrape reviews from Google Play Store.
        
        Args:
            url: Google Play app URL or package ID
            max_reviews: Maximum reviews to collect
            
        Returns:
            List of Review objects
        """
        package_id = self._extract_package_id(url)
        if not package_id:
            logger.error(f"[{self.name}] Could not extract package ID from {url}")
            return []

        logger.info(f"[{self.name}] Scraping reviews for package: {package_id}")
        
        all_reviews = []
        token = None
        
        try:
            while True:
                reviews, token = await self._fetch_review_batch(package_id, token)
                
                if not reviews:
                    break
                    
                all_reviews.extend(reviews)
                logger.info(f"[{self.name}] Fetched {len(reviews)} reviews (total: {len(all_reviews)})")
                
                # Check max limit
                if max_reviews and len(all_reviews) >= max_reviews:
                    all_reviews = all_reviews[:max_reviews]
                    break
                
                # No more pages
                if not token:
                    break
                    
        except Exception as e:
            logger.error(f"[{self.name}] Error scraping {package_id}: {e}")
        
        return all_reviews

    async def _fetch_review_batch(self, package_id: str, token: str | None = None) -> tuple[list[Review], str | None]:
        """Fetch a batch of reviews using Google Play's internal API."""
        
        # Build the request payload
        # This is Google's internal batchexecute format
        if token:
            payload = f'f.req=[[["UsvDTd","[null,null,[2,{self._get_sort_order()},null,null,[null,null,null,null,null,[null,null,null,null,null,null,null,null,null,null,[2]]],[\\\"{package_id}\\\",7],\\\"{token}\\\"]",null,"generic"]]]'
        else:
            payload = f'f.req=[[["UsvDTd","[null,null,[2,{self._get_sort_order()},null,null,[null,null,null,null,null,[null,null,null,null,null,null,null,null,null,null,[2]]],[\\\"{package_id}\\\",7]]",null,"generic"]]]'

        headers = {
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
            "Origin": "https://play.google.com",
            "Referer": f"https://play.google.com/store/apps/details?id={package_id}",
        }
        
        try:
            response = await self.http_client._client.post(
                self.REVIEW_API,
                content=payload,
                headers=headers,
            )
            
            return self._parse_api_response(response.text, package_id)
            
        except Exception as e:
            logger.error(f"[{self.name}] API request failed: {e}")
            return [], None

    def _parse_api_response(self, response_text: str, package_id: str) -> tuple[list[Review], str | None]:
        """Parse the batchexecute API response."""
        reviews = []
        next_token = None
        
        try:
            # Google's response format starts with )]}' which we need to skip
            # Then it's a series of nested arrays
            lines = response_text.split('\n')
            
            for line in lines:
                if line.startswith('['):
                    try:
                        data = json.loads(line)
                        # Navigate the nested structure to find reviews
                        if data and len(data) > 0:
                            inner = data[0]
                            if inner and len(inner) > 2:
                                json_str = inner[2]
                                if json_str:
                                    review_data = json.loads(json_str)
                                    reviews, next_token = self._extract_reviews_from_data(review_data, package_id)
                                    if reviews:
                                        return reviews, next_token
                    except (json.JSONDecodeError, IndexError, TypeError):
                        continue
                        
        except Exception as e:
            logger.warning(f"[{self.name}] Error parsing API response: {e}")
        
        # Fallback: try to extract from HTML page directly
        return reviews, next_token

    def _extract_reviews_from_data(self, data: list, package_id: str) -> tuple[list[Review], str | None]:
        """Extract reviews from parsed API data."""
        reviews = []
        next_token = None
        
        try:
            # The structure varies, but reviews are typically in a nested array
            if not data:
                return reviews, None
                
            # Try to find the reviews array and pagination token
            review_array = None
            
            # Navigate common structures
            if isinstance(data, list) and len(data) > 0:
                # Token is usually at the end
                if len(data) > 1 and isinstance(data[-1], str):
                    next_token = data[-1]
                
                # Reviews are in nested arrays
                for item in data:
                    if isinstance(item, list) and len(item) > 0:
                        # Check if this looks like a review array
                        if self._looks_like_review_array(item):
                            review_array = item
                            break
            
            if review_array:
                for review_item in review_array:
                    review = self._parse_review_item(review_item, package_id)
                    if review:
                        reviews.append(review)
                        
        except Exception as e:
            logger.warning(f"[{self.name}] Error extracting reviews: {e}")
        
        return reviews, next_token

    def _looks_like_review_array(self, item: list) -> bool:
        """Check if an array looks like it contains reviews."""
        if not item or len(item) == 0:
            return False
        first = item[0]
        # Reviews typically have a specific structure with ID, text, rating
        return isinstance(first, list) and len(first) > 4

    def _parse_review_item(self, item: list, package_id: str) -> Review | None:
        """Parse a single review from API data."""
        try:
            if not isinstance(item, list) or len(item) < 5:
                return None
            
            # Extract fields from the array structure
            # Structure: [id, author_name, author_image, [rating], text, ...]
            review_id = item[0] if len(item) > 0 else None
            author = item[1][0] if len(item) > 1 and item[1] else None
            
            # Rating is usually in a nested array
            rating = None
            if len(item) > 3 and item[2]:
                if isinstance(item[2], (int, float)):
                    rating = float(item[2])
                elif isinstance(item[2], list) and len(item[2]) > 0:
                    rating = float(item[2][0])
            
            # Text content
            text = item[4] if len(item) > 4 else None
            
            if not text or len(str(text)) < 5:
                return None
            
            # Timestamp
            date = None
            if len(item) > 5 and item[5]:
                if isinstance(item[5], (int, float)):
                    # Unix timestamp in seconds or milliseconds
                    ts = item[5]
                    if ts > 1e12:  # Milliseconds
                        ts = ts / 1000
                    try:
                        date = datetime.fromtimestamp(ts)
                    except:
                        pass
            
            return self._review_factory.create(
                text=str(text),
                source=self.name,
                source_url=f"https://play.google.com/store/apps/details?id={package_id}",
                source_id=str(review_id) if review_id else None,
                rating=rating,
                date=date,
                author=author,
            )
            
        except Exception as e:
            logger.debug(f"[{self.name}] Error parsing review item: {e}")
            return None

    def _get_sort_order(self) -> int:
        """Get sort order for reviews (2 = newest, 1 = rating, 3 = helpfulness)."""
        return 2  # Newest first

    async def get_pagination_urls(self, base_url: str, max_pages: int | None = None) -> list[str]:
        """Return base URL - pagination is handled internally via tokens."""
        package_id = self._extract_package_id(base_url)
        return [package_id] if package_id else [base_url]

    def parse_review_element(self, element: Tag) -> Review | None:
        """Parse review from HTML element (fallback method)."""
        try:
            # Review text
            text_el = element.select_one("[data-g-id='reviews'] span, .review-text")
            if not text_el:
                return None
            
            text = text_el.get_text(strip=True)
            if not text or len(text) < 10:
                return None
            
            # Rating from stars
            rating = None
            stars_el = element.select_one("[aria-label*='stars'], [aria-label*='Sterne']")
            if stars_el:
                aria = stars_el.get("aria-label", "")
                match = re.search(r"(\d)", aria)
                if match:
                    rating = float(match.group(1))
            
            # Author
            author = None
            author_el = element.select_one(".author-name, [class*='author']")
            if author_el:
                author = author_el.get_text(strip=True)
            
            return self._review_factory.create(
                text=text,
                source=self.name,
                rating=rating,
                author=author,
            )
            
        except Exception as e:
            logger.warning(f"[{self.name}] Error parsing HTML review: {e}")
            return None

    def _extract_package_id(self, url: str) -> str | None:
        """Extract package ID from URL or return as-is if already an ID."""
        # If it looks like a package ID already (e.g., com.example.app)
        if "." in url and "/" not in url and "http" not in url:
            return url
        
        # Extract from URL
        match = re.search(r"[?&]id=([^&]+)", url)
        if match:
            return match.group(1)
        
        # Try to extract from path
        match = re.search(r"/apps/details/([^/?]+)", url)
        if match:
            return match.group(1)
        
        return None

    @staticmethod
    def build_url(package_id: str) -> str:
        """Build Google Play URL from package ID."""
        return f"https://play.google.com/store/apps/details?id={package_id}"

    async def scrape_from_html(self, url: str, max_reviews: int | None = None) -> list[Review]:
        """Fallback: Scrape reviews from HTML page (limited to visible reviews)."""
        try:
            full_url = self.build_url(self._extract_package_id(url)) if not url.startswith("http") else url
            html = await self.http_client.get_text(full_url)
            soup = BeautifulSoup(html, "lxml")
            
            reviews = []
            # Try different selectors for review containers
            containers = soup.select("[data-g-id='reviews'] > div, .review-container, [jscontroller] div[class*='review']")
            
            for container in containers[:max_reviews] if max_reviews else containers:
                review = self.parse_review_element(container)
                if review:
                    reviews.append(review)
            
            return reviews
            
        except Exception as e:
            logger.error(f"[{self.name}] HTML scraping failed: {e}")
            return []


# ADAC App package IDs for convenience
ADAC_APPS = [
    "de.adac.android",           # ADAC Hauptapp
    "de.adac.android.spritpreise", # ADAC Spritpreise  
    "de.adac.android.maps",       # ADAC Maps
    "de.adac.camping",            # ADAC Camping
    "de.adac.android.skiguide",   # ADAC Skiführer
]
