"""
11880.com Scraper - FREE & UNLIMITED

German business directory with reviews.
Simple HTML parsing.
"""

import re
from datetime import datetime
from urllib.parse import quote, urljoin

import requests
from bs4 import BeautifulSoup
from loguru import logger

from src.core.base_scraper import BaseScraper
from src.models.review import Review, ReviewFactory


class Scraper11880(BaseScraper):
    """Scraper for 11880.com reviews."""

    name = "11880"
    base_url = "https://www.11880.com"
    rate_limit_rpm = 30

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._review_factory = ReviewFactory()
        self._session = requests.Session()
        self._session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9',
        })

    def scrape_reviews(self, url: str, max_reviews: int | None = None) -> list[Review]:
        """Scrape reviews from 11880.com."""
        max_reviews = max_reviews or 100
        reviews = []
        
        try:
            if not url.startswith("http"):
                # Search for business
                business_urls = self._search_business(url)
                for biz_url in business_urls[:5]:  # Check first 5 results
                    biz_reviews = self._scrape_business(biz_url, max_reviews - len(reviews))
                    reviews.extend(biz_reviews)
                    if len(reviews) >= max_reviews:
                        break
            else:
                reviews = self._scrape_business(url, max_reviews)
                
        except Exception as e:
            logger.error(f"[{self.name}] Error: {e}")
        
        return reviews

    def _search_business(self, query: str) -> list[str]:
        """Search for businesses."""
        urls = []
        try:
            # Parse query for location
            parts = query.replace("ADAC", "").strip().split()
            if parts:
                location = parts[0]
                search_url = f"{self.base_url}/suche/adac/{quote(location)}"
            else:
                search_url = f"{self.base_url}/suche/adac"
            
            logger.debug(f"[{self.name}] Searching: {search_url}")
            response = self._session.get(search_url, timeout=15)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find business links
            for link in soup.select('a[href*="/branchenbuch/"]'):
                href = link.get('href', '')
                if href and '/branchenbuch/' in href:
                    full_url = urljoin(self.base_url, href)
                    if full_url not in urls:
                        urls.append(full_url)
            
        except Exception as e:
            logger.error(f"[{self.name}] Search error: {e}")
        
        return urls

    def _scrape_business(self, url: str, max_reviews: int) -> list[Review]:
        """Scrape reviews from business page."""
        reviews = []
        
        try:
            response = self._session.get(url, timeout=15)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Get business name
            name_el = soup.select_one('h1, .company-name, [itemprop="name"]')
            business_name = name_el.get_text(strip=True) if name_el else "11880 Business"
            
            # Find reviews
            review_containers = soup.select('.review, .bewertung, [itemprop="review"], .rating-item')
            
            for container in review_containers[:max_reviews]:
                review = self._parse_review(container, business_name, url)
                if review:
                    reviews.append(review)
            
            logger.info(f"[{self.name}] Found {len(reviews)} reviews from {business_name}")
            
        except Exception as e:
            logger.error(f"[{self.name}] Error scraping {url}: {e}")
        
        return reviews

    def _parse_review(self, element, business_name: str, url: str) -> Review | None:
        """Parse review element."""
        try:
            # Get text
            text_el = element.select_one('.review-text, .comment, p, [itemprop="reviewBody"]')
            if not text_el:
                return None
            
            text = text_el.get_text(strip=True)
            if len(text) < 10:
                return None
            
            full_text = f"[{business_name}] {text}"
            
            # Get rating
            rating = None
            rating_el = element.select_one('[itemprop="ratingValue"], .rating, .stars')
            if rating_el:
                rating_text = rating_el.get('content') or rating_el.get_text()
                match = re.search(r'(\d+(?:\.\d+)?)', str(rating_text))
                if match:
                    rating = float(match.group(1))
            
            # Get author
            author = None
            author_el = element.select_one('.author, [itemprop="author"], .reviewer')
            if author_el:
                author = author_el.get_text(strip=True)
            
            return self._review_factory.create(
                text=full_text,
                source=self.name,
                source_url=url,
                rating=rating,
                author=author,
                product_name=business_name,
                metadata={"platform": "11880.com"}
            )
            
        except Exception as e:
            logger.debug(f"[{self.name}] Parse error: {e}")
            return None

    def parse_review_element(self, element) -> Review | None:
        return None

    async def get_pagination_urls(self, base_url: str, max_pages: int | None = None) -> list[str]:
        return [base_url]
