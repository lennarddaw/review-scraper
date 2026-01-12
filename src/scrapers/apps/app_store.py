"""Apple App Store review scraper using app-store-scraper library."""

import asyncio
from datetime import datetime

from loguru import logger

from src.core.base_scraper import BaseScraper
from src.models.review import Review, ReviewFactory

# Try to import the library
try:
    from app_store_scraper import AppStore
    HAS_LIBRARY = True
except ImportError:
    HAS_LIBRARY = False
    logger.warning("app-store-scraper not installed. Run: pip install app-store-scraper")


class AppStoreScraper(BaseScraper):
    """
    Scraper for Apple App Store reviews.
    
    Uses the app-store-scraper library for reliable extraction.
    Install: pip install app-store-scraper
    """

    name = "app_store"
    base_url = "https://apps.apple.com"
    rate_limit_rpm = 30
    requires_browser = False

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._review_factory = ReviewFactory()
        
        if not HAS_LIBRARY:
            logger.error("app-store-scraper library not installed!")

    async def scrape_reviews(self, url: str, max_reviews: int | None = None) -> list[Review]:
        """
        Scrape reviews from Apple App Store.
        
        Args:
            url: App Store URL, app name, or app ID
            max_reviews: Maximum reviews to collect
            
        Returns:
            List of Review objects
        """
        if not HAS_LIBRARY:
            logger.error(f"[{self.name}] app-store-scraper not installed. Run: pip install app-store-scraper")
            return []
            
        app_name, app_id = self._parse_app_info(url)
        if not app_name:
            logger.error(f"[{self.name}] Could not extract app info from {url}")
            return []

        logger.info(f"[{self.name}] Scraping reviews for: {app_name} (ID: {app_id})")
        
        try:
            # Run in thread pool since library is synchronous
            loop = asyncio.get_event_loop()
            
            # Create app store scraper
            app = AppStore(country='de', app_name=app_name, app_id=app_id)
            
            # Fetch reviews
            count = max_reviews or 500
            await loop.run_in_executor(
                None,
                lambda: app.review(how_many=count)
            )
            
            reviews_list = self._parse_reviews(app.reviews, app_name)
            logger.info(f"[{self.name}] Collected {len(reviews_list)} reviews")
            
            if max_reviews:
                reviews_list = reviews_list[:max_reviews]
                
            return reviews_list

        except Exception as e:
            logger.error(f"[{self.name}] Error scraping {app_name}: {e}")
            return []

    def _parse_reviews(self, raw_reviews: list[dict], app_name: str) -> list[Review]:
        """Parse reviews from library response."""
        reviews_list = []
        
        for item in raw_reviews:
            try:
                # Get review text (title + review content)
                title = item.get('title', '').strip()
                content = item.get('review', '').strip()
                
                if title and content:
                    text = f"{title}\n\n{content}"
                else:
                    text = content or title
                    
                if not text or len(text) < 5:
                    continue
                
                # Rating (1-5 scale)
                rating = item.get('rating')
                
                # Date
                date = item.get('date')
                
                # Author
                author = item.get('userName')
                
                review = self._review_factory.create(
                    text=text,
                    source=self.name,
                    source_url=f"https://apps.apple.com/de/app/{app_name}",
                    rating=float(rating) if rating else None,
                    date=date,
                    author=author,
                )
                reviews_list.append(review)
                
            except Exception as e:
                logger.debug(f"[{self.name}] Error parsing review: {e}")
                continue
        
        return reviews_list

    def _parse_app_info(self, url: str) -> tuple[str | None, int | None]:
        """Extract app name and ID from URL or input."""
        import re
        
        # If it's a full URL: https://apps.apple.com/de/app/adac/id397267553
        match = re.search(r'/app/([^/]+)/id(\d+)', url)
        if match:
            return match.group(1), int(match.group(2))
        
        # If it's just an ID
        if url.isdigit():
            return None, int(url)
        
        # If it's a name:id format (e.g., "adac:397267553")
        if ':' in url:
            parts = url.split(':')
            return parts[0], int(parts[1]) if parts[1].isdigit() else None
        
        # Just app name
        return url, None

    async def get_pagination_urls(self, base_url: str, max_pages: int | None = None) -> list[str]:
        """Return base URL - pagination is handled internally."""
        return [base_url]

    def parse_review_element(self, element) -> Review | None:
        """Not used - library handles parsing."""
        return None

    @staticmethod
    def build_url(app_name: str, app_id: int) -> str:
        """Build App Store URL."""
        return f"https://apps.apple.com/de/app/{app_name}/id{app_id}"


# ADAC App Store IDs
ADAC_IOS_APPS = {
    "adac:397267553": "ADAC App",
    "adac-spritpreise:365469498": "ADAC Spritpreise", 
    "adac-maps:1527645928": "ADAC Maps",
    "adac-camping:397304488": "ADAC Camping",
    "adac-trips:1474674498": "ADAC Trips",
}
