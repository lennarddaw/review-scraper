"""Google Play Store app review scraper using google-play-scraper library."""

import asyncio
from datetime import datetime

from loguru import logger

from src.core.base_scraper import BaseScraper
from src.models.review import Review, ReviewFactory

# Try to import the library
try:
    from google_play_scraper import reviews, reviews_all, Sort
    HAS_LIBRARY = True
except ImportError:
    HAS_LIBRARY = False
    logger.warning("google-play-scraper not installed. Run: pip install google-play-scraper")


class GooglePlayScraper(BaseScraper):
    """
    Scraper for Google Play Store app reviews.
    
    Uses the google-play-scraper library for reliable extraction.
    Install: pip install google-play-scraper
    """

    name = "google_play"
    base_url = "https://play.google.com"
    rate_limit_rpm = 30
    requires_browser = False

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._review_factory = ReviewFactory()
        
        if not HAS_LIBRARY:
            logger.error("google-play-scraper library not installed!")

    async def scrape_reviews(self, url: str, max_reviews: int | None = None) -> list[Review]:
        """
        Scrape reviews from Google Play Store.
        
        Args:
            url: Google Play app URL or package ID (e.g., de.adac.android)
            max_reviews: Maximum reviews to collect
            
        Returns:
            List of Review objects
        """
        if not HAS_LIBRARY:
            logger.error(f"[{self.name}] google-play-scraper not installed. Run: pip install google-play-scraper")
            return []
            
        package_id = self._extract_package_id(url)
        if not package_id:
            logger.error(f"[{self.name}] Could not extract package ID from {url}")
            return []

        logger.info(f"[{self.name}] Scraping reviews for package: {package_id}")
        
        try:
            # Run in thread pool since library is synchronous
            loop = asyncio.get_event_loop()
            
            if max_reviews and max_reviews <= 200:
                # Use single batch for small requests
                result, _ = await loop.run_in_executor(
                    None,
                    lambda: reviews(
                        package_id,
                        lang='de',
                        country='de',
                        sort=Sort.NEWEST,
                        count=max_reviews,
                    )
                )
            else:
                # Use reviews_all for larger requests
                count = max_reviews or 5000
                result = await loop.run_in_executor(
                    None,
                    lambda: reviews_all(
                        package_id,
                        lang='de',
                        country='de',
                        sort=Sort.NEWEST,
                        count=count,
                    )
                )
            
            reviews_list = self._parse_reviews(result, package_id)
            logger.info(f"[{self.name}] Collected {len(reviews_list)} reviews")
            
            if max_reviews:
                reviews_list = reviews_list[:max_reviews]
                
            return reviews_list

        except Exception as e:
            logger.error(f"[{self.name}] Error scraping {package_id}: {e}")
            return []

    def _parse_reviews(self, raw_reviews: list[dict], package_id: str) -> list[Review]:
        """Parse reviews from library response."""
        reviews_list = []
        
        for item in raw_reviews:
            try:
                text = item.get('content', '').strip()
                if not text or len(text) < 5:
                    continue
                
                # Rating (1-5 scale)
                rating = item.get('score')
                
                # Date
                date = item.get('at')
                if isinstance(date, str):
                    try:
                        date = datetime.fromisoformat(date)
                    except:
                        date = None
                
                # Author
                author = item.get('userName')
                
                # Helpful count
                helpful = item.get('thumbsUpCount', 0)
                
                # Review ID
                review_id = item.get('reviewId')
                
                review = self._review_factory.create(
                    text=text,
                    source=self.name,
                    source_url=f"https://play.google.com/store/apps/details?id={package_id}",
                    source_id=review_id,
                    rating=float(rating) if rating else None,
                    date=date,
                    author=author,
                    helpful_count=helpful,
                )
                reviews_list.append(review)
                
            except Exception as e:
                logger.debug(f"[{self.name}] Error parsing review: {e}")
                continue
        
        return reviews_list

    async def get_pagination_urls(self, base_url: str, max_pages: int | None = None) -> list[str]:
        """Return base URL - pagination is handled internally."""
        package_id = self._extract_package_id(base_url)
        return [package_id] if package_id else [base_url]

    def parse_review_element(self, element) -> Review | None:
        """Not used - library handles parsing."""
        return None

    def _extract_package_id(self, url: str) -> str | None:
        """Extract package ID from URL or return as-is if already an ID."""
        import re
        
        # If it looks like a package ID (e.g., de.adac.android)
        if "." in url and "/" not in url and "http" not in url:
            return url
        
        # Extract from URL parameter
        match = re.search(r"[?&]id=([^&]+)", url)
        if match:
            return match.group(1)
        
        return None

    @staticmethod
    def build_url(package_id: str) -> str:
        """Build Google Play URL from package ID."""
        return f"https://play.google.com/store/apps/details?id={package_id}"


# ADAC App package IDs
ADAC_APPS = {
    "de.adac.android": "ADAC App",
    "de.adac.android.spritpreise": "ADAC Spritpreise",
    "de.adac.android.maps": "ADAC Maps",
    "de.adac.camping": "ADAC Camping",
    "de.adac.android.skiguide": "ADAC Skiführer",
    "de.adac.android.ausland": "ADAC Auslandshelfer",
}
