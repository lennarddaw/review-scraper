"""Motor-Talk.de forum scraper for German automotive discussions."""

import re
from datetime import datetime
from urllib.parse import urljoin, quote

from bs4 import BeautifulSoup, Tag
from loguru import logger

from src.core.base_scraper import BaseScraper
from src.models.review import Review, ReviewFactory


class MotorTalkScraper(BaseScraper):
    """
    Scraper for Motor-Talk.de automotive forum.
    
    Motor-Talk is Europe's largest automotive community with 50+ million
    members discussing cars, insurance, ADAC, and more.
    
    Search URL: https://www.motor-talk.de/suche.html?q={query}
    """

    name = "motor_talk"
    base_url = "https://www.motor-talk.de"
    rate_limit_rpm = 15
    requires_browser = False

    SELECTORS = {
        # Search results
        "search_result": ".search-result, .searchResult",
        "thread_link": "a[href*='/forum/']",
        
        # Thread page
        "thread_title": "h1, .thread-title",
        "post_container": ".post, .message, article[class*='post']",
        "post_content": ".post-content, .message-content, .messageContent",
        "post_author": ".author, .username, .post-author",
        "post_date": "time, .post-date, .date",
        "reactions": ".reactions, .likes, [class*='reaction']",
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._review_factory = ReviewFactory()

    async def scrape_reviews(self, url: str, max_reviews: int | None = None) -> list[Review]:
        """Scrape discussions from Motor-Talk."""
        try:
            # If it's a search query, build search URL
            if not url.startswith("http"):
                url = f"{self.base_url}/suche.html?q={quote(url)}"
            
            html = await self.http_client.get_text(url)
            
            # Check if it's a search page or thread page
            if "/suche" in url:
                return await self._scrape_search_results(html, url, max_reviews)
            elif "/forum/" in url:
                return self._parse_thread_page(html, url)
            else:
                return []
                
        except Exception as e:
            logger.error(f"[{self.name}] Error scraping {url}: {e}")
            return []

    async def _scrape_search_results(self, html: str, source_url: str, max_reviews: int | None) -> list[Review]:
        """Scrape search results and follow links to threads."""
        soup = BeautifulSoup(html, "lxml")
        reviews = []
        
        # Find all thread links
        thread_links = []
        links = soup.select("a[href*='/forum/'][href*='-t']")
        for link in links:
            href = link.get("href", "")
            if "/forum/" in href and "-t" in href:
                full_url = urljoin(self.base_url, href)
                # Clean URL (remove page params for now)
                full_url = re.sub(r'\?.*$', '', full_url)
                if full_url not in thread_links:
                    thread_links.append(full_url)
        
        logger.info(f"[{self.name}] Found {len(thread_links)} threads to scrape")
        
        # Scrape each thread
        for i, thread_url in enumerate(thread_links):
            if max_reviews and len(reviews) >= max_reviews:
                break
                
            try:
                thread_html = await self.http_client.get_text(thread_url)
                thread_reviews = self._parse_thread_page(thread_html, thread_url)
                reviews.extend(thread_reviews)
                logger.debug(f"[{self.name}] Thread {i+1}: {len(thread_reviews)} posts")
            except Exception as e:
                logger.warning(f"[{self.name}] Error scraping thread {thread_url}: {e}")
                continue
        
        return reviews

    def _parse_thread_page(self, html: str, source_url: str) -> list[Review]:
        """Parse a thread page with all posts."""
        soup = BeautifulSoup(html, "lxml")
        reviews = []
        
        # Get thread title
        thread_title = None
        title_el = soup.select_one("h1")
        if title_el:
            thread_title = title_el.get_text(strip=True)
        
        # Find all posts
        post_containers = soup.select(".post, .message, [class*='message-']")
        
        # Fallback: look for divs with post content
        if not post_containers:
            post_containers = soup.select("div[id^='post'], article")
        
        for container in post_containers:
            review = self._extract_post(container, source_url, thread_title)
            if review:
                reviews.append(review)
        
        return reviews

    def _extract_post(self, container: Tag, source_url: str, thread_title: str | None) -> Review | None:
        """Extract a single forum post."""
        try:
            # Post content
            text = None
            for selector in self.SELECTORS["post_content"].split(", "):
                content_el = container.select_one(selector)
                if content_el:
                    # Remove quotes
                    for quote in content_el.select(".quote, blockquote"):
                        quote.decompose()
                    text = content_el.get_text(strip=True)
                    if text:
                        break
            
            # Fallback: get text directly
            if not text:
                for quote in container.select(".quote, blockquote"):
                    quote.decompose()
                text = container.get_text(strip=True)
            
            if not text or len(text) < 30:
                return None
            
            # Add thread context
            if thread_title:
                full_text = f"[{thread_title}]\n\n{text}"
            else:
                full_text = text
            
            # Author
            author = None
            for selector in self.SELECTORS["post_author"].split(", "):
                author_el = container.select_one(selector)
                if author_el:
                    author = author_el.get_text(strip=True)
                    if author:
                        break
            
            # Date
            date = None
            for selector in self.SELECTORS["post_date"].split(", "):
                date_el = container.select_one(selector)
                if date_el:
                    datetime_attr = date_el.get("datetime")
                    if datetime_attr:
                        try:
                            date = datetime.fromisoformat(datetime_attr.replace("Z", "+00:00"))
                        except:
                            pass
                    if not date:
                        date = self._parse_german_date(date_el.get_text(strip=True))
                    if date:
                        break
            
            # Reactions/likes for rating
            rating = None
            for selector in self.SELECTORS["reactions"].split(", "):
                reactions_el = container.select_one(selector)
                if reactions_el:
                    reaction_text = reactions_el.get_text(strip=True)
                    match = re.search(r"(\d+)", reaction_text)
                    if match:
                        likes = int(match.group(1))
                        if likes >= 10:
                            rating = 5.0
                        elif likes >= 5:
                            rating = 4.0
                        elif likes >= 2:
                            rating = 3.0
                        else:
                            rating = 2.0
                    break
            
            return self._review_factory.create(
                text=full_text,
                source=self.name,
                source_url=source_url,
                title=thread_title,
                author=author,
                date=date,
                rating=rating,
                metadata={"type": "forum_post"},
            )
            
        except Exception as e:
            logger.warning(f"[{self.name}] Error extracting post: {e}")
            return None

    def _parse_german_date(self, text: str) -> datetime | None:
        """Parse German date formats."""
        if not text:
            return None
        
        text_lower = text.lower()
        
        # Relative dates
        if "heute" in text_lower:
            return datetime.now()
        if "gestern" in text_lower:
            from datetime import timedelta
            return datetime.now() - timedelta(days=1)
        
        # German months
        german_months = {
            "januar": 1, "februar": 2, "märz": 3, "april": 4,
            "mai": 5, "juni": 6, "juli": 7, "august": 8,
            "september": 9, "oktober": 10, "november": 11, "dezember": 12,
        }
        
        # Pattern: "12. Februar 2025"
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
        
        # Pattern: "12.02.2025"
        match = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", text)
        if match:
            try:
                return datetime(int(match.group(3)), int(match.group(2)), int(match.group(1)))
            except:
                pass
        
        return None

    def parse_review_element(self, element: Tag) -> Review | None:
        """Not used directly."""
        return None

    async def get_pagination_urls(self, base_url: str, max_pages: int | None = None) -> list[str]:
        """Get pagination URLs."""
        if not base_url.startswith("http"):
            base_url = f"{self.base_url}/suche.html?q={quote(base_url)}"
        
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
        return f"https://www.motor-talk.de/suche.html?q={quote(query)}"


# ADAC-related Motor-Talk threads to scrape
ADAC_THREADS = [
    "https://www.motor-talk.de/forum/eure-erfahrungen-mit-dem-adac-t564580.html",
    "https://www.motor-talk.de/forum/adac-pannendienst-wirklich-so-schlecht-t7068953.html",
    "https://www.motor-talk.de/forum/avd-oder-adac-t8256692.html",
    "https://www.motor-talk.de/forum/alternativen-zum-adac-gesucht-habt-ihr-erfahrungen-t4847103.html",
    "https://www.motor-talk.de/forum/adac-autoversicherung-erfahrung-t6498781.html",
]

# Search queries for ADAC content
ADAC_SEARCH_QUERIES = [
    "ADAC Erfahrungen",
    "ADAC Pannenhilfe",
    "ADAC vs ACE",
    "ADAC Mitgliedschaft",
    "ADAC Versicherung",
]
