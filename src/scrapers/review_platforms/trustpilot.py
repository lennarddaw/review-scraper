"""Trustpilot review scraper."""

import re
from datetime import datetime
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, Tag
from loguru import logger

from src.core.base_scraper import BaseScraper
from src.models.review import Review, ReviewFactory


class TrustpilotScraper(BaseScraper):
    """
    Scraper for Trustpilot reviews.
    
    Trustpilot is one of the easier sites to scrape due to clean HTML structure.
    Reviews are loaded server-side without heavy JavaScript requirements.
    
    URL format: https://www.trustpilot.com/review/{company-name}
    Pagination: ?page=N
    """

    name = "trustpilot"
    base_url = "https://www.trustpilot.com"
    rate_limit_rpm = 15
    requires_browser = False

    # CSS Selectors
    SELECTORS = {
        "review_container": "article.paper_paper__1PY90",
        "review_text": "p.typography_body-l__KUYFJ",
        "review_title": "h2.typography_heading-s__f7029",
        "rating": "div[data-service-review-rating]",
        "date": "time",
        "author": "span[data-consumer-name-typography]",
        "pagination_next": "a[name='pagination-button-next']",
        "total_reviews": "span.typography_body-l__KUYFJ.typography_appearance-default__AAY17",
    }
    
    # Alternative selectors (Trustpilot updates their classes periodically)
    ALT_SELECTORS = {
        "review_container": "[data-service-review-card-paper]",
        "review_text": "[data-service-review-text-typography]",
        "review_title": "[data-service-review-title-typography]",
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._review_factory = ReviewFactory()

    async def scrape_reviews(self, url: str, max_reviews: int | None = None) -> list[Review]:
        """
        Scrape reviews from a Trustpilot page.

        Args:
            url: Trustpilot review page URL
            max_reviews: Maximum reviews to collect from this page

        Returns:
            List of Review objects
        """
        try:
            html = await self.http_client.get_text(url)
            return self._parse_reviews(html, url)
        except Exception as e:
            logger.error(f"[{self.name}] Error scraping {url}: {e}")
            return []

    def _parse_reviews(self, html: str, source_url: str) -> list[Review]:
        """Parse reviews from HTML content."""
        soup = BeautifulSoup(html, "lxml")
        reviews = []

        # Try primary selectors first, then alternatives
        containers = soup.select(self.SELECTORS["review_container"])
        if not containers:
            containers = soup.select(self.ALT_SELECTORS["review_container"])

        logger.debug(f"[{self.name}] Found {len(containers)} review containers")

        for container in containers:
            review = self.parse_review_element(container)
            if review:
                review.source_url = source_url
                reviews.append(review)

        return reviews

    def parse_review_element(self, element: Tag) -> Review | None:
        """
        Parse a single review from a BeautifulSoup element.

        Args:
            element: BeautifulSoup Tag containing the review

        Returns:
            Review object or None if parsing failed
        """
        try:
            # Extract review text (required)
            text_el = (
                element.select_one(self.SELECTORS["review_text"]) or
                element.select_one(self.ALT_SELECTORS["review_text"])
            )
            
            if not text_el:
                return None

            text = text_el.get_text(strip=True)
            if not text:
                return None

            # Extract title
            title_el = (
                element.select_one(self.SELECTORS["review_title"]) or
                element.select_one(self.ALT_SELECTORS["review_title"])
            )
            title = title_el.get_text(strip=True) if title_el else None

            # Combine title and text if both exist
            if title:
                full_text = f"{title}\n\n{text}"
            else:
                full_text = text

            # Extract rating
            rating = None
            rating_el = element.select_one(self.SELECTORS["rating"])
            if rating_el:
                rating_str = rating_el.get("data-service-review-rating", "")
                try:
                    rating = float(rating_str)
                except ValueError:
                    pass

            # Extract date
            date = None
            date_el = element.select_one(self.SELECTORS["date"])
            if date_el:
                datetime_str = date_el.get("datetime", "")
                if datetime_str:
                    try:
                        date = datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))
                    except ValueError:
                        pass

            # Extract author
            author = None
            author_el = element.select_one(self.SELECTORS["author"])
            if author_el:
                author = author_el.get_text(strip=True)

            # Create review
            return self._review_factory.create(
                text=full_text,
                source=self.name,
                title=title,
                rating=rating,
                date=date,
                author=author,
            )

        except Exception as e:
            logger.warning(f"[{self.name}] Error parsing review element: {e}")
            return None

    async def get_pagination_urls(self, base_url: str, max_pages: int | None = None) -> list[str]:
        """
        Get all pagination URLs for a Trustpilot review page.

        Args:
            base_url: Starting URL
            max_pages: Maximum number of pages to collect

        Returns:
            List of page URLs
        """
        urls = [base_url]
        max_pages = max_pages or 100  # Default to 100 pages max
        
        try:
            # Fetch first page to determine total pages
            html = await self.http_client.get_text(base_url)
            total_pages = self._get_total_pages(html)
            
            logger.debug(f"[{self.name}] Detected {total_pages} total pages")
            
            if total_pages <= 1:
                # Fallback: assume there are more pages and try up to max_pages
                # Trustpilot will return 404 or empty for non-existent pages
                total_pages = max_pages

            # Limit pages if specified
            total_pages = min(total_pages, max_pages)

            # Build pagination URLs
            # Trustpilot uses ?page=N format
            parsed = urlparse(base_url)
            base = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            
            for page in range(2, total_pages + 1):
                urls.append(f"{base}?page={page}")

            logger.info(f"[{self.name}] Will scrape up to {len(urls)} pages")

        except Exception as e:
            logger.error(f"[{self.name}] Error getting pagination: {e}")

        return urls

    def _get_total_pages(self, html: str) -> int:
        """Extract total number of pages from HTML."""
        soup = BeautifulSoup(html, "lxml")
        
        # Method 1: Find the last page number link (multiple selector variations)
        page_selectors = [
            "a[name='pagination-button-page']",
            "nav[aria-label*='Pagination'] a",
            "[class*='pagination'] a[href*='page=']",
            "a[href*='page=']",
        ]
        
        for selector in page_selectors:
            page_links = soup.select(selector)
            if page_links:
                pages = []
                for link in page_links:
                    text = link.get_text(strip=True)
                    try:
                        page_num = int(text)
                        pages.append(page_num)
                    except ValueError:
                        # Try to extract from href
                        href = link.get('href', '')
                        match = re.search(r'page=(\d+)', href)
                        if match:
                            pages.append(int(match.group(1)))
                if pages:
                    return max(pages)

        # Method 2: Parse from total reviews count (multiple patterns)
        # Try to find review count in various formats
        count_patterns = [
            r"(\d[\d,.]*)\s*reviews?",
            r"(\d[\d,.]*)\s*Bewertungen",
            r"of\s+([\d,]+)",
            r"von\s+([\d.]+)",
        ]
        
        text = soup.get_text()
        for pattern in count_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                count_str = match.group(1).replace(",", "").replace(".", "")
                try:
                    total_reviews = int(count_str)
                    if total_reviews > 20:
                        # Trustpilot shows 20 reviews per page
                        return (total_reviews + 19) // 20
                except ValueError:
                    continue

        # Method 3: Look for "next" button - indicates more pages
        next_button = soup.select_one("a[name='pagination-button-next'], a[rel='next']")
        if next_button:
            return 50  # Assume at least 50 pages if there's a next button

        # Default to 1 page
        return 1

    @staticmethod
    def build_url(company_name: str) -> str:
        """
        Build a Trustpilot URL from company name.

        Args:
            company_name: Company domain or name (e.g., 'amazon.com' or 'amazon')

        Returns:
            Full Trustpilot review URL
        """
        # Clean up the company name
        company = company_name.lower().strip()
        company = re.sub(r"^https?://", "", company)
        company = re.sub(r"^www\.", "", company)
        company = company.rstrip("/")
        
        return f"https://www.trustpilot.com/review/{company}"
