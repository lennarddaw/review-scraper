"""
Yelp Germany Scraper - FREE & UNLIMITED

Scrapes ADAC reviews from Yelp.de
Simple HTML parsing, no JavaScript required.
"""

import re
from datetime import datetime
from urllib.parse import quote, urljoin

import requests
from bs4 import BeautifulSoup
from loguru import logger

from src.core.base_scraper import BaseScraper
from src.models.review import Review, ReviewFactory


class YelpDeScraper(BaseScraper):
    """Scraper for Yelp.de reviews."""

    name = "yelp_de"
    base_url = "https://www.yelp.de"
    rate_limit_rpm = 30

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._review_factory = ReviewFactory()
        self._session = requests.Session()
        self._session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
        })

    def get_search_url(self, query: str, location: str = "Deutschland") -> str:
        """Build Yelp search URL."""
        return f"{self.base_url}/search?find_desc={quote(query)}&find_loc={quote(location)}"

    def scrape_reviews(self, url: str, max_reviews: int | None = None) -> list[Review]:
        """
        Scrape reviews from Yelp.
        
        Args:
            url: Either a search query (e.g., "ADAC München") 
                 or a direct Yelp business URL
        """
        max_reviews = max_reviews or 100
        reviews = []
        
        try:
            # If it's a search query, find the business first
            if not url.startswith("http"):
                business_url = self._find_business(url)
                if not business_url:
                    logger.warning(f"[{self.name}] No business found for: {url}")
                    return []
            else:
                business_url = url
            
            # Scrape reviews from business page
            reviews = self._scrape_business_reviews(business_url, max_reviews)
            
        except Exception as e:
            logger.error(f"[{self.name}] Error scraping {url}: {e}")
        
        return reviews

    def _find_business(self, query: str) -> str | None:
        """Find business URL from search query."""
        try:
            # Extract location if present (e.g., "ADAC München" -> query="ADAC", location="München")
            parts = query.split()
            if len(parts) > 1:
                search_query = parts[0]  # "ADAC"
                location = " ".join(parts[1:])  # "München"
            else:
                search_query = query
                location = "Deutschland"
            
            search_url = self.get_search_url(search_query, location)
            logger.debug(f"[{self.name}] Searching: {search_url}")
            
            response = self._session.get(search_url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find first business result
            business_links = soup.select('a[href*="/biz/"]')
            for link in business_links:
                href = link.get('href', '')
                if '/biz/' in href and not '/biz_photos/' in href:
                    full_url = urljoin(self.base_url, href.split('?')[0])
                    logger.debug(f"[{self.name}] Found business: {full_url}")
                    return full_url
            
            return None
            
        except Exception as e:
            logger.error(f"[{self.name}] Error finding business: {e}")
            return None

    def _scrape_business_reviews(self, url: str, max_reviews: int) -> list[Review]:
        """Scrape reviews from a business page."""
        reviews = []
        page = 0
        
        while len(reviews) < max_reviews:
            try:
                # Pagination: ?start=0, ?start=10, ?start=20, etc.
                page_url = f"{url}?start={page * 10}" if page > 0 else url
                logger.debug(f"[{self.name}] Fetching page {page + 1}: {page_url}")
                
                response = self._session.get(page_url, timeout=15)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Get business name
                business_name = self._extract_business_name(soup)
                
                # Find review containers
                review_elements = soup.select('[data-review-id], .review, .comment__09f24__D0cxf')
                
                if not review_elements:
                    # Try alternative selectors
                    review_elements = soup.select('li[class*="review"], div[class*="review"]')
                
                if not review_elements:
                    logger.debug(f"[{self.name}] No more reviews found on page {page + 1}")
                    break
                
                page_reviews = 0
                for element in review_elements:
                    if len(reviews) >= max_reviews:
                        break
                    
                    review = self._parse_review(element, business_name, url)
                    if review:
                        reviews.append(review)
                        page_reviews += 1
                
                if page_reviews == 0:
                    break
                
                page += 1
                
                # Safety limit
                if page >= 20:
                    break
                    
            except Exception as e:
                logger.error(f"[{self.name}] Error on page {page + 1}: {e}")
                break
        
        return reviews

    def _extract_business_name(self, soup: BeautifulSoup) -> str:
        """Extract business name from page."""
        selectors = [
            'h1',
            '[data-testid="biz-title"]',
            '.biz-page-title',
        ]
        
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                name = element.get_text(strip=True)
                if name and len(name) > 2:
                    return name
        
        return "Unknown Business"

    def _parse_review(self, element, business_name: str, source_url: str) -> Review | None:
        """Parse a single review element."""
        try:
            # Extract review text
            text = None
            text_selectors = [
                'p[lang="de"]',
                '.comment__09f24__D0cxf p',
                '.review-content p',
                'p[class*="comment"]',
                '.lemon--p',
            ]
            
            for selector in text_selectors:
                text_el = element.select_one(selector)
                if text_el:
                    text = text_el.get_text(strip=True)
                    if text and len(text) > 10:
                        break
            
            if not text or len(text) < 10:
                # Try getting any substantial text
                all_text = element.get_text(strip=True)
                if len(all_text) > 50:
                    text = all_text[:500]
                else:
                    return None
            
            # Add business context
            full_text = f"[{business_name}] {text}"
            
            # Extract rating
            rating = None
            rating_el = element.select_one('[aria-label*="Bewertung"], [aria-label*="rating"], [class*="star"]')
            if rating_el:
                aria = rating_el.get('aria-label', '')
                match = re.search(r'(\d+)', aria)
                if match:
                    rating = float(match.group(1))
            
            # Extract author
            author = None
            author_el = element.select_one('[data-testid="user-name"], .user-name, a[href*="/user_details"]')
            if author_el:
                author = author_el.get_text(strip=True)
            
            # Extract date
            date = None
            date_el = element.select_one('.rating-qualifier, [class*="date"], time')
            if date_el:
                date_text = date_el.get_text(strip=True)
                date = self._parse_date(date_text)
            
            return self._review_factory.create(
                text=full_text,
                source=self.name,
                source_url=source_url,
                rating=rating,
                date=date,
                author=author,
                product_name=business_name,
                metadata={
                    "business_name": business_name,
                    "platform": "yelp.de",
                }
            )
            
        except Exception as e:
            logger.debug(f"[{self.name}] Error parsing review: {e}")
            return None

    def _parse_date(self, text: str) -> datetime | None:
        """Parse German date string."""
        if not text:
            return None
        
        # German month names
        months = {
            'jan': 1, 'feb': 2, 'mär': 3, 'mar': 3, 'apr': 4,
            'mai': 5, 'jun': 6, 'jul': 7, 'aug': 8,
            'sep': 9, 'okt': 10, 'oct': 10, 'nov': 11, 'dez': 12, 'dec': 12
        }
        
        text_lower = text.lower()
        
        # Try "12. Jan. 2024" format
        match = re.search(r'(\d{1,2})\.\s*(\w{3})\w*\.?\s*(\d{4})', text_lower)
        if match:
            day = int(match.group(1))
            month_str = match.group(2)[:3]
            year = int(match.group(3))
            month = months.get(month_str, 1)
            return datetime(year, month, day)
        
        return None

    def parse_review_element(self, element) -> Review | None:
        """Required by base class."""
        return None

    async def get_pagination_urls(self, base_url: str, max_pages: int | None = None) -> list[str]:
        """Return base URL - pagination handled internally."""
        return [base_url]
