"""GoLocal.de local business review scraper."""

import re
from datetime import datetime
from urllib.parse import quote, urljoin

import requests
from bs4 import BeautifulSoup, Tag
from loguru import logger

from src.core.base_scraper import BaseScraper
from src.models.review import Review, ReviewFactory


class GoLocalScraper(BaseScraper):
    """
    Scraper for GoLocal.de business reviews.
    
    GoLocal is a German local business review platform similar to Yelp.
    
    URL format: https://www.golocal.de/suche/?q={query}
    """

    name = "golocal"
    base_url = "https://www.golocal.de"
    rate_limit_rpm = 15
    requires_browser = False

    SELECTORS = {
        "search_result": ".search-result, .result-item",
        "business_link": "a[href*='/firmen/']",
        "business_name": "h1, .company-name, .title",
        "review_container": ".review, .rating-entry, [class*='bewertung']",
        "review_text": ".review-text, .comment, p.text",
        "review_title": ".review-title, h3, h4",
        "rating": ".rating, .stars, [class*='star']",
        "date": ".date, time, .review-date",
        "author": ".author, .username, .reviewer",
    }

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
        """Scrape reviews from GoLocal."""
        max_reviews = max_reviews or 100
        try:
            if not url.startswith("http"):
                url = f"{self.base_url}/suche/?q={quote(url)}"
            
            response = self._session.get(url, timeout=15)
            response.raise_for_status()
            html = response.text
            
            if "/suche/" in url:
                return self._scrape_search_results(html, url, max_reviews)
            else:
                return self._parse_business_page(html, url)
                
        except Exception as e:
            logger.error(f"[{self.name}] Error scraping {url}: {e}")
            return []

    def _scrape_search_results(self, html: str, source_url: str, max_reviews: int) -> list[Review]:
        """Scrape search results and follow business links."""
        soup = BeautifulSoup(html, "html.parser")
        reviews = []
        
        # Find business links
        business_links = []
        links = soup.select("a[href*='/firmen/']")
        for link in links:
            href = link.get("href", "")
            if href:
                full_url = urljoin(self.base_url, href)
                if full_url not in business_links:
                    business_links.append(full_url)
        
        logger.info(f"[{self.name}] Found {len(business_links)} businesses")
        
        for i, business_url in enumerate(business_links[:10]):  # Limit to 10
            if max_reviews and len(reviews) >= max_reviews:
                break
            
            try:
                response = self._session.get(business_url, timeout=15)
                business_reviews = self._parse_business_page(response.text, business_url)
                reviews.extend(business_reviews)
                logger.debug(f"[{self.name}] Business {i+1}: {len(business_reviews)} reviews")
            except Exception as e:
                logger.warning(f"[{self.name}] Error scraping business: {e}")
                continue
        
        return reviews

    def _parse_business_page(self, html: str, source_url: str) -> list[Review]:
        """Parse business page with reviews."""
        soup = BeautifulSoup(html, "html.parser")
        reviews = []
        
        # Get business name
        business_name = None
        for selector in self.SELECTORS["business_name"].split(", "):
            name_el = soup.select_one(selector)
            if name_el:
                business_name = name_el.get_text(strip=True)
                if business_name:
                    break
        
        # Find reviews
        containers = []
        for selector in self.SELECTORS["review_container"].split(", "):
            containers = soup.select(selector)
            if containers:
                break
        
        for container in containers:
            review = self._extract_review(container, source_url, business_name)
            if review:
                reviews.append(review)
        
        return reviews

    def _extract_review(self, container: Tag, source_url: str, business_name: str | None) -> Review | None:
        """Extract a single review."""
        try:
            text_parts = []
            
            # Title
            for selector in self.SELECTORS["review_title"].split(", "):
                title_el = container.select_one(selector)
                if title_el:
                    title = title_el.get_text(strip=True)
                    if title:
                        text_parts.append(title)
                    break
            
            # Review text
            for selector in self.SELECTORS["review_text"].split(", "):
                text_el = container.select_one(selector)
                if text_el:
                    text = text_el.get_text(strip=True)
                    if text:
                        text_parts.append(text)
                    break
            
            full_text = "\n\n".join(text_parts)
            if not full_text or len(full_text) < 15:
                return None
            
            # Add business context
            if business_name:
                full_text = f"[{business_name}] {full_text}"
            
            # Rating
            rating = self._extract_rating(container)
            
            # Date
            date = None
            for selector in self.SELECTORS["date"].split(", "):
                date_el = container.select_one(selector)
                if date_el:
                    date = self._parse_german_date(date_el.get_text(strip=True))
                    if date:
                        break
            
            # Author
            author = None
            for selector in self.SELECTORS["author"].split(", "):
                author_el = container.select_one(selector)
                if author_el:
                    author = author_el.get_text(strip=True)
                    if author:
                        break
            
            return self._review_factory.create(
                text=full_text,
                source=self.name,
                source_url=source_url,
                rating=rating,
                date=date,
                author=author,
                metadata={"business_name": business_name} if business_name else None,
            )
            
        except Exception as e:
            logger.warning(f"[{self.name}] Error extracting review: {e}")
            return None

    def _extract_rating(self, container: Tag) -> float | None:
        """Extract rating from element."""
        for selector in self.SELECTORS["rating"].split(", "):
            rating_el = container.select_one(selector)
            if rating_el:
                # Count filled stars
                filled = rating_el.select("[class*='filled'], [class*='active']")
                if filled:
                    return float(len(filled))
                
                # Check text/attributes
                text = rating_el.get_text(strip=True)
                match = re.search(r"(\d)[,.]?(\d)?", text)
                if match:
                    return float(f"{match.group(1)}.{match.group(2) or 0}")
        
        return None

    def _parse_german_date(self, text: str) -> datetime | None:
        """Parse German date formats."""
        if not text:
            return None
        
        german_months = {
            "januar": 1, "februar": 2, "märz": 3, "april": 4,
            "mai": 5, "juni": 6, "juli": 7, "august": 8,
            "september": 9, "oktober": 10, "november": 11, "dezember": 12,
        }
        
        text_lower = text.lower().strip()
        
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
        
        match = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", text)
        if match:
            try:
                return datetime(int(match.group(3)), int(match.group(2)), int(match.group(1)))
            except:
                pass
        
        return None

    def parse_review_element(self, element: Tag) -> Review | None:
        """Parse review element."""
        return self._extract_review(element, "", None)

    async def get_pagination_urls(self, base_url: str, max_pages: int | None = None) -> list[str]:
        """Get pagination URLs."""
        if not base_url.startswith("http"):
            base_url = f"{self.base_url}/suche/?q={quote(base_url)}"
        
        urls = [base_url]
        max_pages = max_pages or 10
        
        for page in range(2, max_pages + 1):
            if "?" in base_url:
                urls.append(f"{base_url}&page={page}")
            else:
                urls.append(f"{base_url}?page={page}")
        
        return urls

    @staticmethod
    def build_search_url(query: str) -> str:
        """Build search URL."""
        return f"https://www.golocal.de/suche/?q={quote(query)}"
