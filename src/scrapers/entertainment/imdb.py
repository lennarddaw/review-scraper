"""IMDB movie review scraper."""

import re
from datetime import datetime
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, Tag
from loguru import logger

from src.core.base_scraper import BaseScraper
from src.models.review import Review, ReviewFactory


class IMDBScraper(BaseScraper):
    """
    Scraper for IMDB movie reviews.
    
    URL format: https://www.imdb.com/title/{title_id}/reviews
    Pagination: ?paginationKey=... or /reviews/_ajax?paginationKey=...
    
    IMDB has both user reviews and critic reviews. This scraper focuses
    on user reviews which have richer, more emotional content.
    """

    name = "imdb"
    base_url = "https://www.imdb.com"
    rate_limit_rpm = 20
    requires_browser = False

    SELECTORS = {
        "review_container": "div.review-container",
        "review_text": "div.text.show-more__control",
        "review_title": "a.title",
        "rating": "span.rating-other-user-rating span",
        "date": "span.review-date",
        "author": "span.display-name-link a",
        "helpful": "div.actions",
        "load_more": "div.load-more-data",
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._review_factory = ReviewFactory()

    async def scrape_reviews(self, url: str, max_reviews: int | None = None) -> list[Review]:
        """Scrape reviews from an IMDB page."""
        try:
            # Normalize URL - handle title ID input
            url = self._normalize_url(url)
            html = await self.http_client.get_text(url)
            return self._parse_reviews(html, url)
        except Exception as e:
            logger.error(f"[{self.name}] Error scraping {url}: {e}")
            return []

    def _normalize_url(self, url: str) -> str:
        """Convert title ID or partial URL to full reviews URL."""
        # If it's already a full URL
        if url.startswith("http"):
            # Make sure it points to reviews page
            if "/reviews" not in url:
                title_id = self.extract_title_id(url)
                if title_id:
                    return self.build_url(title_id)
            return url
        
        # It's a title ID (e.g., tt0111161)
        return self.build_url(url)

    def _parse_reviews(self, html: str, source_url: str) -> list[Review]:
        """Parse reviews from HTML content."""
        soup = BeautifulSoup(html, "lxml")
        reviews = []

        # Extract movie title for context
        title_el = soup.select_one("h3[itemprop='name'] a, div.parent h3 a")
        movie_title = title_el.get_text(strip=True) if title_el else None

        containers = soup.select(self.SELECTORS["review_container"])
        logger.debug(f"[{self.name}] Found {len(containers)} review containers")

        for container in containers:
            review = self.parse_review_element(container)
            if review:
                review.source_url = source_url
                if movie_title:
                    review.product_name = movie_title
                reviews.append(review)

        return reviews

    def parse_review_element(self, element: Tag) -> Review | None:
        """Parse a single review element."""
        try:
            # Extract review text
            text_el = element.select_one(self.SELECTORS["review_text"])
            if not text_el:
                # Try alternative selector
                text_el = element.select_one("div.content div.text")
            
            if not text_el:
                return None

            text = text_el.get_text(strip=True)
            if not text or len(text) < 10:
                return None

            # Extract title
            title_el = element.select_one(self.SELECTORS["review_title"])
            title = title_el.get_text(strip=True) if title_el else None

            # Combine title and text
            full_text = f"{title}\n\n{text}" if title else text

            # Extract rating (IMDB uses X/10 scale)
            rating = None
            rating_el = element.select_one(self.SELECTORS["rating"])
            if rating_el:
                rating_text = rating_el.get_text(strip=True)
                try:
                    # Convert X/10 to X/5 scale
                    imdb_rating = int(rating_text)
                    rating = imdb_rating / 2.0  # Convert to 5-star scale
                except ValueError:
                    pass

            # Extract date
            date = None
            date_el = element.select_one(self.SELECTORS["date"])
            if date_el:
                date_text = date_el.get_text(strip=True)
                date = self._parse_imdb_date(date_text)

            # Extract author
            author = None
            author_el = element.select_one(self.SELECTORS["author"])
            if author_el:
                author = author_el.get_text(strip=True)

            # Extract helpful count
            helpful_count = None
            helpful_el = element.select_one(self.SELECTORS["helpful"])
            if helpful_el:
                helpful_text = helpful_el.get_text()
                match = re.search(r"(\d+)\s+out\s+of\s+\d+\s+found\s+this\s+helpful", helpful_text)
                if match:
                    helpful_count = int(match.group(1))

            return self._review_factory.create(
                text=full_text,
                source=self.name,
                title=title,
                rating=rating,
                date=date,
                author=author,
                helpful_count=helpful_count,
            )

        except Exception as e:
            logger.warning(f"[{self.name}] Error parsing review: {e}")
            return None

    def _parse_imdb_date(self, date_text: str) -> datetime | None:
        """Parse IMDB date format (e.g., '15 March 2024')."""
        try:
            # Common IMDB date format
            return datetime.strptime(date_text, "%d %B %Y")
        except ValueError:
            try:
                # Alternative format
                return datetime.strptime(date_text, "%B %d, %Y")
            except ValueError:
                return None

    async def get_pagination_urls(self, base_url: str, max_pages: int | None = None) -> list[str]:
        """
        Get pagination URLs for IMDB reviews.
        
        IMDB uses AJAX loading with pagination keys, but we can also
        use the simpler approach of adding sort and filter parameters.
        """
        # Normalize URL first
        base_url = self._normalize_url(base_url)
        urls = [base_url]
        max_pages = max_pages or 25

        # IMDB loads 25 reviews at a time via AJAX
        # For simplicity, we'll just use the main URL which loads initial reviews
        # To get more, you'd need to handle the "Load More" button via browser automation
        
        try:
            html = await self.http_client.get_text(base_url)
            soup = BeautifulSoup(html, "lxml")
            
            # Check for pagination key
            load_more = soup.select_one(self.SELECTORS["load_more"])
            if load_more:
                pagination_key = load_more.get("data-key")
                if pagination_key:
                    # Build AJAX URL for more reviews
                    parsed = urlparse(base_url)
                    title_match = re.search(r"/title/(tt\d+)/", base_url)
                    if title_match:
                        title_id = title_match.group(1)
                        for page in range(1, min(max_pages, 10)):
                            ajax_url = f"{self.base_url}/title/{title_id}/reviews/_ajax?paginationKey={pagination_key}"
                            urls.append(ajax_url)
                            # Note: Each AJAX call returns a new pagination key
                            # For full implementation, you'd need to chain these

        except Exception as e:
            logger.error(f"[{self.name}] Pagination error: {e}")

        return urls[:max_pages] if max_pages else urls

    @staticmethod
    def build_url(title_id: str) -> str:
        """
        Build IMDB review URL from title ID.

        Args:
            title_id: IMDB title ID (e.g., 'tt0111161')

        Returns:
            Full IMDB reviews URL
        """
        if not title_id.startswith("tt"):
            title_id = f"tt{title_id}"
        return f"https://www.imdb.com/title/{title_id}/reviews"

    @staticmethod
    def extract_title_id(url: str) -> str | None:
        """Extract title ID from IMDB URL."""
        match = re.search(r"/title/(tt\d+)", url)
        return match.group(1) if match else None
