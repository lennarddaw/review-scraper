"""Gelbe Seiten (German Yellow Pages) review scraper."""

import re
from datetime import datetime
from urllib.parse import quote, urljoin

import requests
from bs4 import BeautifulSoup, Tag
from loguru import logger

from src.core.base_scraper import BaseScraper
from src.models.review import Review, ReviewFactory


class GelbeSeitenScraper(BaseScraper):
    """
    Scraper for Gelbe Seiten (German Yellow Pages) reviews.
    
    Gelbe Seiten has business reviews that are easier to scrape than Google Maps.
    
    URL format: https://www.gelbeseiten.de/suche/{query}
    """

    name = "gelbe_seiten"
    base_url = "https://www.gelbeseiten.de"
    rate_limit_rpm = 15
    requires_browser = False

    SELECTORS = {
        "search_result": ".teilnehmer, .mod-Treffer",
        "business_link": "a[href*='/gsbiz/']",
        "business_name": "h1, .name, [data-wipe-name]",
        "review_container": ".review, .bewertung, [class*='review']",
        "review_text": ".review-text, .bewertung-text, p",
        "rating": ".rating, .sterne, [class*='star']",
        "date": ".date, .datum, time",
        "author": ".author, .name, .reviewer",
        "overall_rating": ".gesamtbewertung, .overall-rating",
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
        """Scrape reviews from Gelbe Seiten."""
        max_reviews = max_reviews or 100
        
        try:
            # If it's a search query, build URL
            if not url.startswith("http"):
                url = f"{self.base_url}/suche/{quote(url)}"
            
            response = self._session.get(url, timeout=15)
            response.raise_for_status()
            html = response.text
            
            # Check if it's a search page or business page
            if "/suche/" in url:
                return self._scrape_search_results(html, url, max_reviews)
            else:
                return self._parse_business_page(html, url)
                
        except Exception as e:
            logger.error(f"[{self.name}] Error scraping {url}: {e}")
            return []

    def _scrape_search_results(self, html: str, source_url: str, max_reviews: int) -> list[Review]:
        """Scrape search results and follow links to businesses."""
        soup = BeautifulSoup(html, "html.parser")
        reviews = []
        
        # Find business links
        business_links = []
        for selector in self.SELECTORS["business_link"].split(", "):
            links = soup.select(selector)
            for link in links:
                href = link.get("href", "")
                if href:
                    full_url = urljoin(self.base_url, href)
                    if full_url not in business_links:
                        business_links.append(full_url)
        
        logger.info(f"[{self.name}] Found {len(business_links)} businesses")
        
        # Scrape each business
        for i, business_url in enumerate(business_links[:10]):  # Limit to 10 businesses
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
        """Parse a business page with reviews."""
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
        
        # Find review containers
        containers = []
        for selector in self.SELECTORS["review_container"].split(", "):
            containers = soup.select(selector)
            if containers:
                break
        
        for container in containers:
            review = self._extract_review(container, source_url, business_name)
            if review:
                reviews.append(review)
        
        # If no individual reviews, try to get overall rating as feedback
        if not reviews:
            overall = self._extract_overall_rating(soup, source_url, business_name)
            if overall:
                reviews.append(overall)
        
        return reviews

    def _extract_review(self, container: Tag, source_url: str, business_name: str | None) -> Review | None:
        """Extract a single review."""
        try:
            # Review text
            text = None
            for selector in self.SELECTORS["review_text"].split(", "):
                text_el = container.select_one(selector)
                if text_el:
                    text = text_el.get_text(strip=True)
                    if text and len(text) > 10:
                        break
            
            if not text or len(text) < 10:
                return None
            
            # Add business context
            if business_name:
                text = f"[{business_name}] {text}"
            
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
                text=text,
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
        """Extract rating from various formats."""
        # Try star count
        for selector in self.SELECTORS["rating"].split(", "):
            rating_el = container.select_one(selector)
            if rating_el:
                # Check for filled stars
                filled = rating_el.select("[class*='filled'], [class*='active'], .stern-voll")
                if filled:
                    return float(len(filled))
                
                # Check text content
                text = rating_el.get_text(strip=True)
                match = re.search(r"(\d)[,.]?(\d)?", text)
                if match:
                    return float(f"{match.group(1)}.{match.group(2) or 0}")
                
                # Check class names
                classes = ' '.join(rating_el.get('class', []))
                match = re.search(r'(\d)', classes)
                if match:
                    return float(match.group(1))
        
        return None

    def _extract_overall_rating(self, soup: BeautifulSoup, source_url: str, business_name: str | None) -> Review | None:
        """Extract overall rating as a summary review."""
        for selector in self.SELECTORS["overall_rating"].split(", "):
            rating_el = soup.select_one(selector)
            if rating_el:
                text = rating_el.get_text(strip=True)
                rating = self._extract_rating(rating_el)
                
                if business_name:
                    summary_text = f"[{business_name}] Gesamtbewertung: {text}"
                else:
                    summary_text = f"Gesamtbewertung: {text}"
                
                return self._review_factory.create(
                    text=summary_text,
                    source=self.name,
                    source_url=source_url,
                    rating=rating,
                    metadata={"type": "overall_rating"},
                )
        
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
        
        # Pattern: "27. November 2025"
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
        
        # Pattern: "27.11.2025"
        match = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", text)
        if match:
            try:
                return datetime(int(match.group(3)), int(match.group(2)), int(match.group(1)))
            except:
                pass
        
        return None

    def parse_review_element(self, element: Tag) -> Review | None:
        """Parse review element - calls _extract_review."""
        return self._extract_review(element, "", None)

    async def get_pagination_urls(self, base_url: str, max_pages: int | None = None) -> list[str]:
        """Get pagination URLs."""
        if not base_url.startswith("http"):
            base_url = f"{self.base_url}/suche/{quote(base_url)}"
        
        urls = [base_url]
        max_pages = max_pages or 10
        
        for page in range(2, max_pages + 1):
            if "?" in base_url:
                urls.append(f"{base_url}&seite={page}")
            else:
                urls.append(f"{base_url}?seite={page}")
        
        return urls

    @staticmethod
    def build_search_url(query: str) -> str:
        """Build search URL."""
        return f"https://www.gelbeseiten.de/suche/{quote(query)}"


# ADAC search queries for Gelbe Seiten
ADAC_SEARCHES = [
    "ADAC",
    "ADAC Geschäftsstelle",
    "ADAC Reisebüro",
    "ADAC Fahrsicherheitszentrum",
    "ADAC Prüfzentrum",
]
