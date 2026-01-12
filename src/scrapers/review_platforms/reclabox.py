"""Reclabox complaint scraper (German complaint platform)."""

import re
from datetime import datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag
from loguru import logger

from src.core.base_scraper import BaseScraper
from src.models.review import Review, ReviewFactory


class ReclaboxScraper(BaseScraper):
    """
    Scraper for Reclabox complaints.
    
    Reclabox is a German platform for consumer complaints.
    Very valuable for negative feedback and issues.
    
    URL format: https://www.reclabox.com/unternehmen/{company-id}/{company-slug}
    """

    name = "reclabox"
    base_url = "https://www.reclabox.com"
    rate_limit_rpm = 15
    requires_browser = False

    SELECTORS = {
        "complaint_container": ".complaint-item, article.complaint, .beschwerde",
        "complaint_title": "h2 a, h3.complaint-title, .complaint-title",
        "complaint_text": ".complaint-text, .complaint-content, .beschwerde-text",
        "date": ".complaint-date, time, .date",
        "status": ".complaint-status, .status-badge",
        "category": ".complaint-category, .category",
        "pagination": "a.page-link[rel='next'], .pagination a.next",
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._review_factory = ReviewFactory()

    async def scrape_reviews(self, url: str, max_reviews: int | None = None) -> list[Review]:
        """Scrape complaints from Reclabox."""
        try:
            url = self._normalize_url(url)
            html = await self.http_client.get_text(url)
            return self._parse_complaints(html, url)
        except Exception as e:
            logger.error(f"[{self.name}] Error scraping {url}: {e}")
            return []

    def _normalize_url(self, url: str) -> str:
        """Normalize URL to company complaints page."""
        if not url.startswith("http"):
            # Search URL for company
            url = f"{self.base_url}/suche?q={url}"
        return url

    def _parse_complaints(self, html: str, source_url: str) -> list[Review]:
        """Parse complaints from HTML content."""
        soup = BeautifulSoup(html, "lxml")
        reviews = []

        # Extract company name
        company_el = soup.select_one("h1, .company-name")
        company_name = company_el.get_text(strip=True) if company_el else None

        # Find complaint containers
        containers = []
        for selector in self.SELECTORS["complaint_container"].split(", "):
            containers = soup.select(selector)
            if containers:
                break

        logger.debug(f"[{self.name}] Found {len(containers)} complaint containers")

        for container in containers:
            review = self.parse_review_element(container)
            if review:
                review.source_url = source_url
                if company_name:
                    review.product_name = company_name
                # Complaints are inherently negative
                if review.rating is None:
                    review.rating = 1.0
                reviews.append(review)

        return reviews

    def parse_review_element(self, element: Tag) -> Review | None:
        """Parse a single complaint element."""
        try:
            text_parts = []
            
            # Title
            title = None
            for selector in self.SELECTORS["complaint_title"].split(", "):
                title_el = element.select_one(selector)
                if title_el:
                    title = title_el.get_text(strip=True)
                    if title:
                        text_parts.append(f"**{title}**")
                    break

            # Main complaint text
            for selector in self.SELECTORS["complaint_text"].split(", "):
                text_el = element.select_one(selector)
                if text_el:
                    text = text_el.get_text(strip=True)
                    if text:
                        text_parts.append(text)
                    break

            # Category
            for selector in self.SELECTORS["category"].split(", "):
                cat_el = element.select_one(selector)
                if cat_el:
                    cat = cat_el.get_text(strip=True)
                    if cat:
                        text_parts.append(f"Kategorie: {cat}")
                    break

            full_text = "\n\n".join(text_parts)
            if not full_text or len(full_text) < 20:
                return None

            # Date
            date = None
            for selector in self.SELECTORS["date"].split(", "):
                date_el = element.select_one(selector)
                if date_el:
                    date_text = date_el.get_text(strip=True)
                    date = self._parse_date(date_text)
                    if date:
                        break

            # Status (resolved, unresolved, etc.)
            status = None
            for selector in self.SELECTORS["status"].split(", "):
                status_el = element.select_one(selector)
                if status_el:
                    status = status_el.get_text(strip=True)
                    break

            # Rating based on status (complaints are negative by nature)
            rating = 1.0
            if status:
                status_lower = status.lower()
                if "gelöst" in status_lower or "resolved" in status_lower:
                    rating = 2.0  # Slightly better if resolved

            return self._review_factory.create(
                text=full_text,
                source=self.name,
                title=title,
                rating=rating,
                date=date,
                metadata={"status": status} if status else None,
            )

        except Exception as e:
            logger.warning(f"[{self.name}] Error parsing complaint: {e}")
            return None

    def _parse_date(self, date_text: str) -> datetime | None:
        """Parse German date formats."""
        if not date_text:
            return None
        
        formats = [
            "%d.%m.%Y",
            "%d.%m.%y",
            "%d. %B %Y",
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
            if "?" in base_url:
                urls.append(f"{base_url}&page={page}")
            else:
                urls.append(f"{base_url}?page={page}")
        
        return urls

    @staticmethod
    def build_url(company_slug: str) -> str:
        """Build search URL for company."""
        return f"https://www.reclabox.com/suche?q={company_slug}"
