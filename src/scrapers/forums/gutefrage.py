"""Gutefrage.net Q&A scraper for German discussions."""

import re
from datetime import datetime
from urllib.parse import urljoin, quote

from bs4 import BeautifulSoup, Tag
from loguru import logger

from src.core.base_scraper import BaseScraper
from src.models.review import Review, ReviewFactory


class GutefrageScraper(BaseScraper):
    """
    Scraper for Gutefrage.net Q&A discussions.
    
    Gutefrage is Germany's largest Q&A platform with millions of questions
    and answers on every topic including ADAC, insurance, cars, etc.
    
    Search URL: https://www.gutefrage.net/suche?q={query}
    """

    name = "gutefrage"
    base_url = "https://www.gutefrage.net"
    rate_limit_rpm = 20
    requires_browser = False

    SELECTORS = {
        # Search results
        "search_result": ".SearchResult, .Question, article[class*='Question']",
        "question_link": "a[href*='/frage/']",
        
        # Question page
        "question_title": "h1, .QuestionHeader__title",
        "question_text": ".QuestionDetail__text, .Question__text, [data-qa='question-detail-text']",
        "answer_container": ".Answer, [data-qa='answer']",
        "answer_text": ".Answer__text, .AnswerText, [data-qa='answer-text']",
        "answer_rating": ".Answer__rating, [data-qa='answer-rating']",
        "date": "time, .DateTime, [data-qa='date']",
        "author": ".Username, .Author__name, [data-qa='username']",
        "tags": ".Tag, .QuestionTags a",
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._review_factory = ReviewFactory()

    async def scrape_reviews(self, url: str, max_reviews: int | None = None) -> list[Review]:
        """
        Scrape discussions from Gutefrage.
        
        Args:
            url: Search query or full URL
            max_reviews: Maximum items to collect
            
        Returns:
            List of Review objects (questions + answers combined)
        """
        try:
            # If it's a search query, build search URL
            if not url.startswith("http"):
                url = f"{self.base_url}/suche?q={quote(url)}"
            
            html = await self.http_client.get_text(url)
            
            # Check if it's a search page or question page
            if "/suche" in url or "/tag/" in url:
                return await self._scrape_search_results(html, url, max_reviews)
            else:
                return self._parse_question_page(html, url)
                
        except Exception as e:
            logger.error(f"[{self.name}] Error scraping {url}: {e}")
            return []

    async def _scrape_search_results(self, html: str, source_url: str, max_reviews: int | None) -> list[Review]:
        """Scrape search results and follow links to questions."""
        soup = BeautifulSoup(html, "lxml")
        reviews = []
        
        # Find all question links
        question_links = []
        for selector in self.SELECTORS["question_link"].split(", "):
            links = soup.select(selector)
            for link in links:
                href = link.get("href", "")
                if "/frage/" in href:
                    full_url = urljoin(self.base_url, href)
                    if full_url not in question_links:
                        question_links.append(full_url)
        
        logger.info(f"[{self.name}] Found {len(question_links)} questions to scrape")
        
        # Scrape each question page
        for i, question_url in enumerate(question_links):
            if max_reviews and len(reviews) >= max_reviews:
                break
                
            try:
                question_html = await self.http_client.get_text(question_url)
                question_reviews = self._parse_question_page(question_html, question_url)
                reviews.extend(question_reviews)
                logger.debug(f"[{self.name}] Question {i+1}: {len(question_reviews)} items")
            except Exception as e:
                logger.warning(f"[{self.name}] Error scraping question {question_url}: {e}")
                continue
        
        return reviews

    def _parse_question_page(self, html: str, source_url: str) -> list[Review]:
        """Parse a single question page with all answers."""
        soup = BeautifulSoup(html, "lxml")
        reviews = []
        
        # Extract question
        question_review = self._extract_question(soup, source_url)
        if question_review:
            reviews.append(question_review)
        
        # Extract all answers
        answer_containers = []
        for selector in self.SELECTORS["answer_container"].split(", "):
            answer_containers = soup.select(selector)
            if answer_containers:
                break
        
        for container in answer_containers:
            answer_review = self._extract_answer(container, source_url)
            if answer_review:
                reviews.append(answer_review)
        
        return reviews

    def _extract_question(self, soup: BeautifulSoup, source_url: str) -> Review | None:
        """Extract the main question."""
        try:
            # Title
            title = None
            for selector in self.SELECTORS["question_title"].split(", "):
                title_el = soup.select_one(selector)
                if title_el:
                    title = title_el.get_text(strip=True)
                    if title:
                        break
            
            # Question text
            text = None
            for selector in self.SELECTORS["question_text"].split(", "):
                text_el = soup.select_one(selector)
                if text_el:
                    text = text_el.get_text(strip=True)
                    if text:
                        break
            
            # Combine title and text
            if title and text:
                full_text = f"[Frage] {title}\n\n{text}"
            elif title:
                full_text = f"[Frage] {title}"
            elif text:
                full_text = f"[Frage] {text}"
            else:
                return None
            
            if len(full_text) < 20:
                return None
            
            # Date
            date = self._extract_date(soup)
            
            # Tags
            tags = []
            for selector in self.SELECTORS["tags"].split(", "):
                tag_els = soup.select(selector)
                for tag_el in tag_els:
                    tag = tag_el.get_text(strip=True)
                    if tag:
                        tags.append(tag)
            
            return self._review_factory.create(
                text=full_text,
                source=self.name,
                source_url=source_url,
                title=title,
                date=date,
                metadata={"type": "question", "tags": tags} if tags else {"type": "question"},
            )
            
        except Exception as e:
            logger.warning(f"[{self.name}] Error extracting question: {e}")
            return None

    def _extract_answer(self, container: Tag, source_url: str) -> Review | None:
        """Extract a single answer."""
        try:
            # Answer text
            text = None
            for selector in self.SELECTORS["answer_text"].split(", "):
                text_el = container.select_one(selector)
                if text_el:
                    text = text_el.get_text(strip=True)
                    if text:
                        break
            
            # Fallback: get all text from container
            if not text:
                text = container.get_text(strip=True)
            
            if not text or len(text) < 20:
                return None
            
            full_text = f"[Antwort] {text}"
            
            # Author
            author = None
            for selector in self.SELECTORS["author"].split(", "):
                author_el = container.select_one(selector)
                if author_el:
                    author = author_el.get_text(strip=True)
                    if author:
                        break
            
            # Rating (helpful votes)
            rating = None
            for selector in self.SELECTORS["answer_rating"].split(", "):
                rating_el = container.select_one(selector)
                if rating_el:
                    rating_text = rating_el.get_text(strip=True)
                    match = re.search(r"(\d+)", rating_text)
                    if match:
                        # Normalize to 1-5 scale based on helpful votes
                        votes = int(match.group(1))
                        if votes >= 10:
                            rating = 5.0
                        elif votes >= 5:
                            rating = 4.0
                        elif votes >= 2:
                            rating = 3.0
                        elif votes >= 1:
                            rating = 2.0
                        else:
                            rating = 1.0
                    break
            
            # Check for "beste Antwort" (best answer)
            is_best = False
            if container.select_one("[class*='best'], [class*='Best'], .hilfreichste"):
                is_best = True
                rating = 5.0
            
            return self._review_factory.create(
                text=full_text,
                source=self.name,
                source_url=source_url,
                author=author,
                rating=rating,
                metadata={"type": "answer", "is_best": is_best},
            )
            
        except Exception as e:
            logger.warning(f"[{self.name}] Error extracting answer: {e}")
            return None

    def _extract_date(self, soup: BeautifulSoup) -> datetime | None:
        """Extract date from page."""
        for selector in self.SELECTORS["date"].split(", "):
            date_el = soup.select_one(selector)
            if date_el:
                # Try datetime attribute
                datetime_attr = date_el.get("datetime")
                if datetime_attr:
                    try:
                        return datetime.fromisoformat(datetime_attr.replace("Z", "+00:00"))
                    except:
                        pass
                
                # Try text parsing
                date_text = date_el.get_text(strip=True)
                date = self._parse_german_date(date_text)
                if date:
                    return date
        
        return None

    def _parse_german_date(self, text: str) -> datetime | None:
        """Parse German date formats."""
        if not text:
            return None
        
        # Handle relative dates
        text_lower = text.lower()
        if "heute" in text_lower:
            return datetime.now()
        if "gestern" in text_lower:
            from datetime import timedelta
            return datetime.now() - timedelta(days=1)
        
        # German month names
        german_months = {
            "januar": 1, "februar": 2, "märz": 3, "april": 4,
            "mai": 5, "juni": 6, "juli": 7, "august": 8,
            "september": 9, "oktober": 10, "november": 11, "dezember": 12,
        }
        
        # Try pattern: "27. November 2025"
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
        
        # Try pattern: "27.11.2025"
        match = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", text)
        if match:
            try:
                return datetime(int(match.group(3)), int(match.group(2)), int(match.group(1)))
            except:
                pass
        
        return None

    def parse_review_element(self, element: Tag) -> Review | None:
        """Parse review from element - not used directly."""
        return None

    async def get_pagination_urls(self, base_url: str, max_pages: int | None = None) -> list[str]:
        """Get pagination URLs for search results."""
        # If it's a search query, build URL
        if not base_url.startswith("http"):
            base_url = f"{self.base_url}/suche?q={quote(base_url)}"
        
        urls = [base_url]
        max_pages = max_pages or 20
        
        # Gutefrage uses page parameter
        for page in range(2, max_pages + 1):
            if "?" in base_url:
                urls.append(f"{base_url}&page={page}")
            else:
                urls.append(f"{base_url}?page={page}")
        
        return urls

    @staticmethod
    def build_search_url(query: str) -> str:
        """Build search URL for query."""
        return f"https://www.gutefrage.net/suche?q={quote(query)}"


# Useful ADAC search queries
ADAC_QUERIES = [
    "ADAC Erfahrungen",
    "ADAC Pannenhilfe",
    "ADAC Mitgliedschaft",
    "ADAC Plus",
    "ADAC Premium",
    "ADAC Versicherung",
    "ADAC Abschleppen",
    "ADAC Alternative",
    "ADAC Wartezeit",
    "ADAC Fahrsicherheitstraining",
    "ADAC Gebrauchtwagencheck",
    "ADAC App",
    "ADAC Camping",
    "ADAC Reise",
]
