"""
WerKenntDenBesten.de Scraper - FREE & UNLIMITED

Large German review platform with many local business reviews.
"""

import re
from datetime import datetime
from urllib.parse import quote, urljoin

import requests
from bs4 import BeautifulSoup
from loguru import logger

from src.core.base_scraper import BaseScraper
from src.models.review import Review, ReviewFactory


class WerKenntDenBestenScraper(BaseScraper):
    """Scraper for WerKenntDenBesten.de reviews."""

    name = "werkenntdenbesten"
    base_url = "https://www.werkenntdenbesten.de"
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
                for biz_url in business_urls[:10]:
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
        """Search for ADAC businesses."""
        urls = []
        try:
            # Extract city
            query_clean = query.replace("ADAC", "").replace("Geschäftsstelle", "").strip()
            city = query_clean.split()[0] if query_clean else "München"
            
            # WerKenntDenBesten search URL format
            search_url = f"{self.base_url}/suche/{quote(city.lower())}/adac"
            logger.debug(f"[{self.name}] Searching: {search_url}")
            
            response = self._session.get(search_url, timeout=15)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find business profile links
            for link in soup.select('a[href*="/profil/"], a[href*="/firma/"]'):
                href = link.get('href', '')
                full_url = urljoin(self.base_url, href.split('?')[0])
                if full_url not in urls:
                    urls.append(full_url)
            
        except Exception as e:
            logger.error(f"[{self.name}] Search error: {e}")
        
        return urls

    def _scrape_business(self, url: str, max_reviews: int) -> list[Review]:
        """Scrape reviews from business profile."""
        reviews = []
        page = 1
        
        while len(reviews) < max_reviews:
            try:
                page_url = f"{url}?page={page}" if page > 1 else url
                response = self._session.get(page_url, timeout=15)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Business name
                if page == 1:
                    name_el = soup.select_one('h1, .profile-name, .company-name')
                    business_name = name_el.get_text(strip=True) if name_el else "Business"
                    self._current_business = business_name
                else:
                    business_name = getattr(self, '_current_business', 'Business')
                
                # Find reviews
                review_containers = soup.select('.review, .bewertung, .rating-entry, [itemprop="review"]')
                
                if not review_containers:
                    break
                
                page_count = 0
                for container in review_containers:
                    if len(reviews) >= max_reviews:
                        break
                    review = self._parse_review(container, business_name, url)
                    if review:
                        reviews.append(review)
                        page_count += 1
                
                if page_count == 0:
                    break
                
                page += 1
                if page > 10:  # Safety limit
                    break
                    
            except Exception as e:
                logger.error(f"[{self.name}] Error on page {page}: {e}")
                break
        
        logger.info(f"[{self.name}] Found {len(reviews)} reviews")
        return reviews

    def _parse_review(self, element, business_name: str, url: str) -> Review | None:
        """Parse review element."""
        try:
            # Text
            text = None
            for selector in ['.review-text', '.text', '.comment', 'p', '[itemprop="reviewBody"]']:
                text_el = element.select_one(selector)
                if text_el:
                    text = text_el.get_text(strip=True)
                    if len(text) > 10:
                        break
            
            if not text or len(text) < 10:
                return None
            
            full_text = f"[{business_name}] {text}"
            
            # Rating
            rating = None
            rating_el = element.select_one('.rating, .score, [itemprop="ratingValue"], .stars')
            if rating_el:
                content = rating_el.get('content') or rating_el.get_text()
                match = re.search(r'(\d+(?:[.,]\d+)?)', str(content))
                if match:
                    rating = float(match.group(1).replace(',', '.'))
            
            # Author
            author = None
            author_el = element.select_one('.author, .reviewer, [itemprop="author"]')
            if author_el:
                author = author_el.get_text(strip=True)
            
            # Date
            date = None
            date_el = element.select_one('.date, time, [itemprop="datePublished"]')
            if date_el:
                date_str = date_el.get('datetime') or date_el.get_text()
                date = self._parse_date(date_str)
            
            return self._review_factory.create(
                text=full_text,
                source=self.name,
                source_url=url,
                rating=rating,
                author=author,
                date=date,
                product_name=business_name,
                metadata={"platform": "werkenntdenbesten.de"}
            )
            
        except Exception as e:
            logger.debug(f"[{self.name}] Parse error: {e}")
            return None

    def _parse_date(self, text: str) -> datetime | None:
        """Parse date string."""
        if not text:
            return None
        
        # Try ISO format
        try:
            return datetime.fromisoformat(text.split('T')[0])
        except:
            pass
        
        # Try German format
        match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', text)
        if match:
            return datetime(int(match.group(3)), int(match.group(2)), int(match.group(1)))
        
        return None

    def parse_review_element(self, element) -> Review | None:
        return None

    async def get_pagination_urls(self, base_url: str, max_pages: int | None = None) -> list[str]:
        return [base_url]
