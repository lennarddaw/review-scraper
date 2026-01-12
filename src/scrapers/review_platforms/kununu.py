"""Kununu company review scraper (German Glassdoor equivalent)."""

import re
from datetime import datetime
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, Tag
from loguru import logger

from src.core.base_scraper import BaseScraper
from src.models.review import Review, ReviewFactory


class KununuScraper(BaseScraper):
    """
    Scraper for Kununu company reviews.
    
    Kununu is the leading employer review platform in German-speaking countries.
    Reviews include employee experiences, salary info, and company culture.
    
    URL format: https://www.kununu.com/de/{company-slug}
    Reviews: https://www.kununu.com/de/{company-slug}/kommentare
    """

    name = "kununu"
    base_url = "https://www.kununu.com"
    rate_limit_rpm = 15
    requires_browser = False

    SELECTORS = {
        "review_container": "article[class*='review-'], div[data-testid='review-item'], .review-item",
        "review_text": "[data-testid='review-text'], .review-text, .review-description p",
        "review_title": "[data-testid='review-title'], .review-title h3, h3.review-title",
        "rating": "[data-testid='rating-score'], .rating-score, .score",
        "date": "[data-testid='review-date'], .review-date, time",
        "author_position": "[data-testid='reviewer-job-title'], .reviewer-job-title",
        "author_department": "[data-testid='reviewer-department'], .reviewer-department", 
        "pros": "[data-testid='review-pros'], .review-pros, div[class*='pro']",
        "cons": "[data-testid='review-cons'], .review-cons, div[class*='con']",
        "pagination": "a[data-testid='pagination-next'], .pagination a[rel='next'], a.next",
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._review_factory = ReviewFactory()

    async def scrape_reviews(self, url: str, max_reviews: int | None = None) -> list[Review]:
        """Scrape reviews from Kununu."""
        try:
            url = self._normalize_url(url)
            html = await self.http_client.get_text(url)
            return self._parse_reviews(html, url)
        except Exception as e:
            logger.error(f"[{self.name}] Error scraping {url}: {e}")
            return []

    def _normalize_url(self, url: str) -> str:
        """Ensure URL points to the reviews/comments page."""
        if not url.startswith("http"):
            # It's a company slug
            url = f"{self.base_url}/de/{url}"
        
        # Make sure we're on the comments page
        if "/kommentare" not in url and "/bewertungen" not in url:
            url = url.rstrip("/") + "/kommentare"
        
        return url

    def _parse_reviews(self, html: str, source_url: str) -> list[Review]:
        """Parse reviews from HTML content."""
        soup = BeautifulSoup(html, "lxml")
        reviews = []

        # Extract company name
        company_el = soup.select_one("h1, [data-testid='company-name']")
        company_name = company_el.get_text(strip=True) if company_el else None

        # Find review containers - try multiple selectors
        containers = []
        for selector in self.SELECTORS["review_container"].split(", "):
            containers = soup.select(selector)
            if containers:
                break
        
        # Also try finding by structure
        if not containers:
            containers = soup.find_all("article")
        
        logger.debug(f"[{self.name}] Found {len(containers)} review containers")

        for container in containers:
            review = self.parse_review_element(container)
            if review:
                review.source_url = source_url
                if company_name:
                    review.product_name = company_name
                reviews.append(review)

        return reviews

    def parse_review_element(self, element: Tag) -> Review | None:
        """Parse a single review element."""
        try:
            # Build review text from multiple parts
            text_parts = []
            
            # Main review title
            title = None
            for selector in self.SELECTORS["review_title"].split(", "):
                title_el = element.select_one(selector)
                if title_el:
                    title = title_el.get_text(strip=True)
                    if title:
                        text_parts.append(f"**{title}**")
                    break

            # Main description/text
            for selector in self.SELECTORS["review_text"].split(", "):
                text_el = element.select_one(selector)
                if text_el:
                    text = text_el.get_text(strip=True)
                    if text:
                        text_parts.append(text)
                    break

            # Pros (Gut am Arbeitgeber)
            for selector in self.SELECTORS["pros"].split(", "):
                pros_el = element.select_one(selector)
                if pros_el:
                    pros_text = pros_el.get_text(strip=True)
                    if pros_text and len(pros_text) > 5:
                        text_parts.append(f"Pro: {pros_text}")
                    break

            # Cons (Schlecht am Arbeitgeber)
            for selector in self.SELECTORS["cons"].split(", "):
                cons_el = element.select_one(selector)
                if cons_el:
                    cons_text = cons_el.get_text(strip=True)
                    if cons_text and len(cons_text) > 5:
                        text_parts.append(f"Contra: {cons_text}")
                    break

            # Combine all text
            full_text = "\n\n".join(text_parts)
            if not full_text or len(full_text) < 20:
                return None

            # Extract rating (Kununu uses 1-5 scale)
            rating = None
            for selector in self.SELECTORS["rating"].split(", "):
                rating_el = element.select_one(selector)
                if rating_el:
                    rating_text = rating_el.get_text(strip=True)
                    # Try to extract number
                    match = re.search(r"(\d[.,]?\d?)", rating_text)
                    if match:
                        rating = float(match.group(1).replace(",", "."))
                        break
            
            # Also check for star ratings
            if not rating:
                stars = element.select("[class*='star'][class*='filled'], .star.active")
                if stars:
                    rating = float(len(stars))

            # Extract date
            date = None
            for selector in self.SELECTORS["date"].split(", "):
                date_el = element.select_one(selector)
                if date_el:
                    date_text = date_el.get_text(strip=True)
                    date = self._parse_date(date_text)
                    if not date:
                        # Try datetime attribute
                        date_attr = date_el.get("datetime")
                        if date_attr:
                            date = self._parse_date(date_attr)
                    if date:
                        break

            # Extract author info (job title + department)
            author_parts = []
            for selector in self.SELECTORS["author_position"].split(", "):
                pos_el = element.select_one(selector)
                if pos_el:
                    pos = pos_el.get_text(strip=True)
                    if pos:
                        author_parts.append(pos)
                    break
            
            for selector in self.SELECTORS["author_department"].split(", "):
                dept_el = element.select_one(selector)
                if dept_el:
                    dept = dept_el.get_text(strip=True)
                    if dept:
                        author_parts.append(dept)
                    break
            
            author = " - ".join(author_parts) if author_parts else None

            return self._review_factory.create(
                text=full_text,
                source=self.name,
                title=title,
                rating=rating,
                date=date,
                author=author,
            )

        except Exception as e:
            logger.warning(f"[{self.name}] Error parsing review: {e}")
            return None

    def _parse_date(self, date_text: str) -> datetime | None:
        """Parse various German date formats."""
        if not date_text:
            return None
            
        date_text = date_text.strip()
        
        # Try ISO format first
        try:
            return datetime.fromisoformat(date_text.replace("Z", "+00:00"))
        except ValueError:
            pass
        
        # German date formats
        formats = [
            "%d.%m.%Y",
            "%d. %B %Y",
            "%B %Y",
            "%d %B %Y",
            "%Y-%m-%d",
        ]
        
        # German month names
        german_months = {
            "januar": "January", "februar": "February", "märz": "March",
            "april": "April", "mai": "May", "juni": "June",
            "juli": "July", "august": "August", "september": "September",
            "oktober": "October", "november": "November", "dezember": "December",
        }
        
        date_lower = date_text.lower()
        for de, en in german_months.items():
            date_lower = date_lower.replace(de, en)
        
        for fmt in formats:
            try:
                return datetime.strptime(date_lower, fmt)
            except ValueError:
                continue
        
        return None

    async def get_pagination_urls(self, base_url: str, max_pages: int | None = None) -> list[str]:
        """Get pagination URLs for Kununu reviews."""
        base_url = self._normalize_url(base_url)
        urls = [base_url]
        max_pages = max_pages or 50
        
        try:
            # Kununu uses page parameter
            for page in range(2, max_pages + 1):
                if "?" in base_url:
                    page_url = f"{base_url}&page={page}"
                else:
                    page_url = f"{base_url}?page={page}"
                urls.append(page_url)
                
        except Exception as e:
            logger.error(f"[{self.name}] Pagination error: {e}")
        
        return urls

    @staticmethod  
    def build_url(company_slug: str, country: str = "de") -> str:
        """Build Kununu URL from company slug."""
        return f"https://www.kununu.com/{country}/{company_slug}/kommentare"

    @staticmethod
    def extract_company_slug(url: str) -> str | None:
        """Extract company slug from Kununu URL."""
        match = re.search(r"kununu\.com/\w+/([^/]+)", url)
        return match.group(1) if match else None


# Known ADAC Kununu URLs
ADAC_KUNUNU = [
    "adac",
    "adac-ev",
    "adac-versicherung",
]
