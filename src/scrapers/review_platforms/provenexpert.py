"""ProvenExpert business review scraper."""

import re
from datetime import datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag
from loguru import logger

from src.core.base_scraper import BaseScraper
from src.models.review import Review, ReviewFactory


class ProvenExpertScraper(BaseScraper):
    """
    Scraper for ProvenExpert business reviews.
    
    ProvenExpert is a German B2B/B2C review platform.
    
    URL format: https://www.provenexpert.com/{company-slug}
    """

    name = "provenexpert"
    base_url = "https://www.provenexpert.com"
    rate_limit_rpm = 15
    requires_browser = False

    SELECTORS = {
        "review_container": ".review-item, .rating-entry, [class*='review']",
        "review_text": ".review-text, .rating-text, [class*='review-content']",
        "rating": ".rating-stars, [class*='stars'], .score",
        "date": ".review-date, time, .date",
        "author": ".reviewer-name, .author",
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._review_factory = ReviewFactory()

    async def scrape_reviews(self, url: str, max_reviews: int | None = None) -> list[Review]:
        """Scrape reviews from ProvenExpert."""
        try:
            url = self._normalize_url(url)
            html = await self.http_client.get_text(url)
            return self._parse_reviews(html, url)
        except Exception as e:
            logger.error(f"[{self.name}] Error scraping {url}: {e}")
            return []

    def _normalize_url(self, url: str) -> str:
        """Normalize URL to company page."""
        if not url.startswith("http"):
            url = f"{self.base_url}/{url}"
        return url

    def _parse_reviews(self, html: str, source_url: str) -> list[Review]:
        """Parse reviews from HTML content."""
        soup = BeautifulSoup(html, "lxml")
        reviews = []

        # Try to find review containers
        containers = []
        for selector in self.SELECTORS["review_container"].split(", "):
            containers = soup.select(selector)
            if containers:
                break

        logger.debug(f"[{self.name}] Found {len(containers)} review containers")

        for container in containers:
            review = self.parse_review_element(container)
            if review:
                review.source_url = source_url
                reviews.append(review)

        return reviews

    def parse_review_element(self, element: Tag) -> Review | None:
        """Parse a single review element."""
        try:
            # Get review text
            text = None
            for selector in self.SELECTORS["review_text"].split(", "):
                text_el = element.select_one(selector)
                if text_el:
                    text = text_el.get_text(strip=True)
                    if text:
                        break

            if not text or len(text) < 10:
                return None

            # Get rating
            rating = None
            for selector in self.SELECTORS["rating"].split(", "):
                rating_el = element.select_one(selector)
                if rating_el:
                    # Try to extract from class or data attribute
                    rating_class = ' '.join(rating_el.get('class', []))
                    match = re.search(r'(\d)[,.]?(\d)?', rating_class)
                    if match:
                        rating = float(f"{match.group(1)}.{match.group(2) or 0}")
                    else:
                        # Count filled stars
                        stars = rating_el.select("[class*='filled'], [class*='active']")
                        if stars:
                            rating = float(len(stars))
                    if rating:
                        break

            # Get date
            date = None
            for selector in self.SELECTORS["date"].split(", "):
                date_el = element.select_one(selector)
                if date_el:
                    date_text = date_el.get_text(strip=True)
                    date = self._parse_date(date_text)
                    if date:
                        break

            # Get author
            author = None
            for selector in self.SELECTORS["author"].split(", "):
                author_el = element.select_one(selector)
                if author_el:
                    author = author_el.get_text(strip=True)
                    if author:
                        break

            return self._review_factory.create(
                text=text,
                source=self.name,
                rating=rating,
                date=date,
                author=author,
            )

        except Exception as e:
            logger.warning(f"[{self.name}] Error parsing review: {e}")
            return None

    def _parse_date(self, date_text: str) -> datetime | None:
        """Parse date text."""
        if not date_text:
            return None
            
        formats = [
            "%d.%m.%Y",
            "%d. %B %Y",
            "%Y-%m-%d",
        ]
        
        german_months = {
            "januar": "January", "februar": "February", "märz": "March",
            "april": "April", "mai": "May", "juni": "June",
            "juli": "July", "august": "August", "september": "September",
            "oktober": "October", "november": "November", "dezember": "December",
        }
        
        date_lower = date_text.lower().strip()
        for de, en in german_months.items():
            date_lower = date_lower.replace(de, en)
        
        for fmt in formats:
            try:
                return datetime.strptime(date_lower, fmt)
            except ValueError:
                continue
        
        return None

    async def get_pagination_urls(self, base_url: str, max_pages: int | None = None) -> list[str]:
        """Get pagination URLs."""
        base_url = self._normalize_url(base_url)
        urls = [base_url]
        max_pages = max_pages or 20
        
        for page in range(2, max_pages + 1):
            urls.append(f"{base_url}?page={page}")
        
        return urls

    @staticmethod
    def build_url(company_slug: str) -> str:
        """Build ProvenExpert URL."""
        return f"https://www.provenexpert.com/{company_slug}"
