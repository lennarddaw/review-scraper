"""
KennstDuEinen.de Scraper - FREE & UNLIMITED

German service provider review platform.
Popular for reviewing local services including automotive clubs.
"""

import re
from datetime import datetime
from urllib.parse import quote, urljoin

import requests
from bs4 import BeautifulSoup
from loguru import logger

from src.core.base_scraper import BaseScraper
from src.models.review import Review, ReviewFactory


class KennstDuEinenScraper(BaseScraper):
    """Scraper for KennstDuEinen.de reviews."""

    name = "kennstdueinen"
    base_url = "https://www.kennstdueinen.de"
    rate_limit_rpm = 30

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._review_factory = ReviewFactory()
        self._session = requests.Session()
        self._session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'de-DE,de;q=0.9',
        })

    def scrape_reviews(self, url: str, max_reviews: int | None = None) -> list[Review]:
        """Scrape reviews."""
        max_reviews = max_reviews or 100
        reviews = []
        
        try:
            if not url.startswith("http"):
                business_urls = self._search_business(url)
                for biz_url in business_urls[:5]:
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
            query_clean = query.replace("ADAC", "").replace("Geschäftsstelle", "").strip()
            city = query_clean.split()[0] if query_clean else ""
            
            search_url = f"{self.base_url}/suche?q=ADAC&loc={quote(city)}"
            logger.debug(f"[{self.name}] Searching: {search_url}")
            
            response = self._session.get(search_url, timeout=15)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            for link in soup.select('a[href*="/bewertung/"]'):
                href = link.get('href', '')
                if '/bewertung/' in href:
                    full_url = urljoin(self.base_url, href.split('?')[0])
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
            
            name_el = soup.select_one('h1, .company-name')
            business_name = name_el.get_text(strip=True) if name_el else "KennstDuEinen Business"
            
            review_containers = soup.select('.review, .rating-item, .bewertung-item, article.review')
            
            for container in review_containers[:max_reviews]:
                review = self._parse_review(container, business_name, url)
                if review:
                    reviews.append(review)
            
            logger.info(f"[{self.name}] Found {len(reviews)} reviews from {business_name}")
            
        except Exception as e:
            logger.error(f"[{self.name}] Error: {e}")
        
        return reviews

    def _parse_review(self, element, business_name: str, url: str) -> Review | None:
        """Parse review element."""
        try:
            text_el = element.select_one('.review-text, .comment, .text, p')
            if not text_el:
                return None
            
            text = text_el.get_text(strip=True)
            if len(text) < 10:
                return None
            
            full_text = f"[{business_name}] {text}"
            
            rating = None
            rating_el = element.select_one('.rating, .score, .stars')
            if rating_el:
                match = re.search(r'(\d+(?:\.\d+)?)', rating_el.get_text())
                if match:
                    rating = float(match.group(1))
            
            author = None
            author_el = element.select_one('.author, .reviewer, .username')
            if author_el:
                author = author_el.get_text(strip=True)
            
            return self._review_factory.create(
                text=full_text,
                source=self.name,
                source_url=url,
                rating=rating,
                author=author,
                product_name=business_name,
                metadata={"platform": "kennstdueinen.de"}
            )
            
        except Exception as e:
            logger.debug(f"[{self.name}] Parse error: {e}")
            return None

    def parse_review_element(self, element) -> Review | None:
        return None

    async def get_pagination_urls(self, base_url: str, max_pages: int | None = None) -> list[str]:
        return [base_url]
