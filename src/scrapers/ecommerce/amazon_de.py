"""Amazon.de product review scraper."""

import re
from datetime import datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag
from loguru import logger

from src.core.base_scraper import BaseScraper
from src.models.review import Review, ReviewFactory


class AmazonDEScraper(BaseScraper):
    """
    Scraper for Amazon.de product reviews.
    
    Useful for ADAC-related products like:
    - ADAC Reiseführer
    - ADAC Camping guides
    - Car accessories
    - Travel guides
    
    URL format: https://www.amazon.de/product-reviews/{ASIN}
    """

    name = "amazon_de"
    base_url = "https://www.amazon.de"
    rate_limit_rpm = 10  # Amazon is strict
    requires_browser = False

    SELECTORS = {
        "review_container": "[data-hook='review'], .review, .a-section.review",
        "review_title": "[data-hook='review-title'], .review-title, .a-text-bold",
        "review_text": "[data-hook='review-body'], .review-text, .reviewText",
        "rating": "[data-hook='review-star-rating'], .review-rating, i[data-hook*='star']",
        "date": "[data-hook='review-date'], .review-date",
        "author": ".a-profile-name, .author",
        "helpful_votes": "[data-hook='helpful-vote-statement']",
        "verified": "[data-hook='avp-badge'], .avp-badge",
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._review_factory = ReviewFactory()
        # Add German headers
        self._extra_headers = {
            "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
        }

    async def scrape_reviews(self, url: str, max_reviews: int | None = None) -> list[Review]:
        """Scrape reviews from Amazon.de product page."""
        try:
            url = self._normalize_url(url)
            html = await self.http_client.get_text(url, headers=self._extra_headers)
            return self._parse_reviews(html, url)
        except Exception as e:
            logger.error(f"[{self.name}] Error scraping {url}: {e}")
            return []

    def _normalize_url(self, url: str) -> str:
        """Normalize URL to reviews page."""
        # If it's an ASIN, build review URL
        if re.match(r'^[A-Z0-9]{10}$', url):
            return f"{self.base_url}/product-reviews/{url}"
        
        # If it's a product URL, convert to reviews URL
        if "/dp/" in url:
            match = re.search(r'/dp/([A-Z0-9]{10})', url)
            if match:
                return f"{self.base_url}/product-reviews/{match.group(1)}"
        
        # If already a review URL
        if "/product-reviews/" in url:
            return url
        
        return url

    def _parse_reviews(self, html: str, source_url: str) -> list[Review]:
        """Parse reviews from HTML."""
        soup = BeautifulSoup(html, "lxml")
        reviews = []
        
        # Get product name
        product_name = None
        product_el = soup.select_one("[data-hook='product-link'], .product-title")
        if product_el:
            product_name = product_el.get_text(strip=True)
        
        # Find review containers
        containers = soup.select(self.SELECTORS["review_container"])
        
        logger.debug(f"[{self.name}] Found {len(containers)} review containers")
        
        for container in containers:
            review = self.parse_review_element(container)
            if review:
                review.source_url = source_url
                if product_name:
                    review.product_name = product_name
                reviews.append(review)
        
        return reviews

    def parse_review_element(self, element: Tag) -> Review | None:
        """Parse a single review element."""
        try:
            # Title
            title = None
            for selector in self.SELECTORS["review_title"].split(", "):
                title_el = element.select_one(selector)
                if title_el:
                    # Amazon often wraps title in multiple elements
                    title = title_el.get_text(strip=True)
                    # Remove "X von 5 Sternen" prefix
                    title = re.sub(r'^\d+[.,]\d*\s*von\s*\d+\s*Sternen?\s*', '', title)
                    if title:
                        break
            
            # Review text
            text = None
            for selector in self.SELECTORS["review_text"].split(", "):
                text_el = element.select_one(selector)
                if text_el:
                    text = text_el.get_text(strip=True)
                    if text:
                        break
            
            if not text or len(text) < 10:
                return None
            
            # Combine title and text
            if title and title not in text:
                full_text = f"{title}\n\n{text}"
            else:
                full_text = text
            
            # Rating
            rating = None
            for selector in self.SELECTORS["rating"].split(", "):
                rating_el = element.select_one(selector)
                if rating_el:
                    # Try aria-label
                    aria = rating_el.get("aria-label", "")
                    match = re.search(r"(\d+)[.,]?(\d*)\s*von\s*5", aria)
                    if match:
                        rating = float(f"{match.group(1)}.{match.group(2) or 0}")
                    else:
                        # Try class name
                        classes = ' '.join(rating_el.get('class', []))
                        match = re.search(r'a-star-(\d)', classes)
                        if match:
                            rating = float(match.group(1))
                    if rating:
                        break
            
            # Date
            date = None
            for selector in self.SELECTORS["date"].split(", "):
                date_el = element.select_one(selector)
                if date_el:
                    date = self._parse_german_date(date_el.get_text(strip=True))
                    if date:
                        break
            
            # Author
            author = None
            for selector in self.SELECTORS["author"].split(", "):
                author_el = element.select_one(selector)
                if author_el:
                    author = author_el.get_text(strip=True)
                    if author:
                        break
            
            # Verified purchase
            verified = False
            for selector in self.SELECTORS["verified"].split(", "):
                if element.select_one(selector):
                    verified = True
                    break
            
            # Helpful votes
            helpful = 0
            for selector in self.SELECTORS["helpful_votes"].split(", "):
                helpful_el = element.select_one(selector)
                if helpful_el:
                    helpful_text = helpful_el.get_text(strip=True)
                    match = re.search(r"(\d+)", helpful_text.replace(".", ""))
                    if match:
                        helpful = int(match.group(1))
                    break
            
            return self._review_factory.create(
                text=full_text,
                source=self.name,
                title=title,
                rating=rating,
                date=date,
                author=author,
                helpful_count=helpful,
                metadata={"verified_purchase": verified},
            )
            
        except Exception as e:
            logger.warning(f"[{self.name}] Error parsing review: {e}")
            return None

    def _parse_german_date(self, text: str) -> datetime | None:
        """Parse German Amazon date format."""
        if not text:
            return None
        
        # Pattern: "Rezension aus Deutschland vom 27. November 2025"
        german_months = {
            "januar": 1, "februar": 2, "märz": 3, "april": 4,
            "mai": 5, "juni": 6, "juli": 7, "august": 8,
            "september": 9, "oktober": 10, "november": 11, "dezember": 12,
        }
        
        text_lower = text.lower()
        match = re.search(r"(\d{1,2})\.\s*(\w+)\s*(\d{4})", text_lower)
        if match:
            day = int(match.group(1))
            month_name = match.group(2)
            year = int(match.group(3))
            month = german_months.get(month_name)
            if month:
                try:
                    return datetime(year, month, day)
                except:
                    pass
        
        return None

    async def get_pagination_urls(self, base_url: str, max_pages: int | None = None) -> list[str]:
        """Get pagination URLs for Amazon reviews."""
        base_url = self._normalize_url(base_url)
        urls = [base_url]
        max_pages = max_pages or 10
        
        # Extract ASIN
        match = re.search(r'/product-reviews/([A-Z0-9]{10})', base_url)
        if match:
            asin = match.group(1)
            for page in range(2, max_pages + 1):
                urls.append(f"{self.base_url}/product-reviews/{asin}?pageNumber={page}")
        
        return urls

    @staticmethod
    def build_url(asin: str) -> str:
        """Build Amazon review URL from ASIN."""
        return f"https://www.amazon.de/product-reviews/{asin}"


# ADAC-related products on Amazon.de
ADAC_PRODUCTS = {
    "B0CVXR5KMN": "ADAC Reiseatlas Deutschland",
    "B0D1QHXM1K": "ADAC Camping Guide",
    "B09XJZD8V1": "ADAC Autobatterieladegerät",
    "B07DLGJFRL": "ADAC Warnweste Set",
}
