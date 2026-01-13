"""Reddit scraper for German subreddits using old.reddit.com."""

import re
from datetime import datetime
from urllib.parse import urljoin, quote

from bs4 import BeautifulSoup, Tag
from loguru import logger

from src.core.base_scraper import BaseScraper
from src.models.review import Review, ReviewFactory


class RedditScraper(BaseScraper):
    """
    Scraper for Reddit posts and comments.
    
    Uses old.reddit.com for easier HTML scraping.
    Useful for German subreddits: r/de, r/germany, r/finanzen, r/autos
    
    Search URL: https://old.reddit.com/r/{subreddit}/search?q={query}&restrict_sr=on
    """

    name = "reddit"
    base_url = "https://old.reddit.com"
    rate_limit_rpm = 20
    requires_browser = False

    SELECTORS = {
        # Search/listing
        "post_listing": ".thing.link",
        "post_title": "a.title",
        "post_link": "a.comments",
        
        # Post page
        "post_content": ".usertext-body .md",
        "comment": ".comment",
        "comment_body": ".usertext-body .md",
        "author": ".author",
        "score": ".score.unvoted, .score.likes, .score.dislikes",
        "time": "time",
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._review_factory = ReviewFactory()

    async def scrape_reviews(self, url: str, max_reviews: int | None = None) -> list[Review]:
        """Scrape posts and comments from Reddit."""
        try:
            # Build search URL if needed
            if not url.startswith("http"):
                # Assume it's a search query for r/de
                url = f"{self.base_url}/r/de/search?q={quote(url)}&restrict_sr=on&sort=relevance"
            
            # Ensure we use old.reddit.com
            url = url.replace("www.reddit.com", "old.reddit.com")
            url = url.replace("reddit.com", "old.reddit.com")
            
            html = await self.http_client.get_text(url)
            
            # Check if it's a search/listing page or a post page
            if "/search" in url or url.endswith("/") or "/r/" in url and "/comments/" not in url:
                return await self._scrape_listing(html, url, max_reviews)
            else:
                return self._parse_post_page(html, url)
                
        except Exception as e:
            logger.error(f"[{self.name}] Error scraping {url}: {e}")
            return []

    async def _scrape_listing(self, html: str, source_url: str, max_reviews: int | None) -> list[Review]:
        """Scrape listing page and follow links to posts."""
        soup = BeautifulSoup(html, "lxml")
        reviews = []
        
        # Find all post links
        post_links = []
        posts = soup.select(self.SELECTORS["post_listing"])
        
        for post in posts:
            # Get comments link
            link_el = post.select_one(self.SELECTORS["post_link"])
            if link_el:
                href = link_el.get("href", "")
                if href:
                    full_url = href if href.startswith("http") else urljoin(self.base_url, href)
                    full_url = full_url.replace("www.reddit.com", "old.reddit.com")
                    post_links.append(full_url)
        
        logger.info(f"[{self.name}] Found {len(post_links)} posts to scrape")
        
        # Scrape each post
        for i, post_url in enumerate(post_links):
            if max_reviews and len(reviews) >= max_reviews:
                break
                
            try:
                post_html = await self.http_client.get_text(post_url)
                post_reviews = self._parse_post_page(post_html, post_url)
                reviews.extend(post_reviews)
                logger.debug(f"[{self.name}] Post {i+1}: {len(post_reviews)} items")
            except Exception as e:
                logger.warning(f"[{self.name}] Error scraping post {post_url}: {e}")
                continue
        
        return reviews

    def _parse_post_page(self, html: str, source_url: str) -> list[Review]:
        """Parse a Reddit post page with comments."""
        soup = BeautifulSoup(html, "lxml")
        reviews = []
        
        # Extract post title
        title = None
        title_el = soup.select_one("a.title, .top-matter .title a")
        if title_el:
            title = title_el.get_text(strip=True)
        
        # Extract main post
        post_review = self._extract_post(soup, source_url, title)
        if post_review:
            reviews.append(post_review)
        
        # Extract comments
        comments = soup.select(".comment")
        for comment in comments:
            comment_review = self._extract_comment(comment, source_url, title)
            if comment_review:
                reviews.append(comment_review)
        
        return reviews

    def _extract_post(self, soup: BeautifulSoup, source_url: str, title: str | None) -> Review | None:
        """Extract the main post content."""
        try:
            # Get post body
            content_el = soup.select_one(".expando .usertext-body .md")
            text = ""
            
            if content_el:
                text = content_el.get_text(strip=True)
            
            # Combine title and text
            if title and text:
                full_text = f"[Post] {title}\n\n{text}"
            elif title:
                full_text = f"[Post] {title}"
            elif text:
                full_text = f"[Post] {text}"
            else:
                return None
            
            if len(full_text) < 20:
                return None
            
            # Score
            score = None
            score_el = soup.select_one(".score.unvoted")
            if score_el:
                score_text = score_el.get("title", "") or score_el.get_text(strip=True)
                match = re.search(r"(\d+)", score_text.replace(".", "").replace(",", ""))
                if match:
                    score = int(match.group(1))
            
            # Date
            date = None
            time_el = soup.select_one(".top-matter time, .tagline time")
            if time_el:
                datetime_attr = time_el.get("datetime")
                if datetime_attr:
                    try:
                        date = datetime.fromisoformat(datetime_attr.replace("Z", "+00:00"))
                    except:
                        pass
            
            # Author
            author = None
            author_el = soup.select_one(".top-matter .author")
            if author_el:
                author = author_el.get_text(strip=True)
            
            # Convert score to rating (1-5)
            rating = None
            if score is not None:
                if score >= 100:
                    rating = 5.0
                elif score >= 50:
                    rating = 4.0
                elif score >= 10:
                    rating = 3.0
                elif score >= 1:
                    rating = 2.0
                else:
                    rating = 1.0
            
            return self._review_factory.create(
                text=full_text,
                source=self.name,
                source_url=source_url,
                title=title,
                author=author,
                date=date,
                rating=rating,
                metadata={"type": "post", "score": score},
            )
            
        except Exception as e:
            logger.warning(f"[{self.name}] Error extracting post: {e}")
            return None

    def _extract_comment(self, container: Tag, source_url: str, post_title: str | None) -> Review | None:
        """Extract a single comment."""
        try:
            # Comment text
            text_el = container.select_one(".usertext-body .md")
            if not text_el:
                return None
            
            text = text_el.get_text(strip=True)
            if not text or len(text) < 20:
                return None
            
            full_text = f"[Kommentar] {text}"
            
            # Author
            author = None
            author_el = container.select_one(".author")
            if author_el:
                author = author_el.get_text(strip=True)
            
            # Score
            score = None
            score_el = container.select_one(".score.unvoted, .score")
            if score_el:
                score_text = score_el.get("title", "") or score_el.get_text(strip=True)
                match = re.search(r"(-?\d+)", score_text.replace(".", "").replace(",", ""))
                if match:
                    score = int(match.group(1))
            
            # Date
            date = None
            time_el = container.select_one("time")
            if time_el:
                datetime_attr = time_el.get("datetime")
                if datetime_attr:
                    try:
                        date = datetime.fromisoformat(datetime_attr.replace("Z", "+00:00"))
                    except:
                        pass
            
            # Convert score to rating
            rating = None
            if score is not None:
                if score >= 50:
                    rating = 5.0
                elif score >= 20:
                    rating = 4.0
                elif score >= 5:
                    rating = 3.0
                elif score >= 1:
                    rating = 2.0
                else:
                    rating = 1.0
            
            return self._review_factory.create(
                text=full_text,
                source=self.name,
                source_url=source_url,
                author=author,
                date=date,
                rating=rating,
                metadata={"type": "comment", "score": score, "post_title": post_title},
            )
            
        except Exception as e:
            logger.warning(f"[{self.name}] Error extracting comment: {e}")
            return None

    def parse_review_element(self, element: Tag) -> Review | None:
        """Not used directly."""
        return None

    async def get_pagination_urls(self, base_url: str, max_pages: int | None = None) -> list[str]:
        """Get pagination URLs."""
        # Reddit uses after= parameter for pagination
        # For now, just return the base URL
        return [base_url]

    @staticmethod
    def build_search_url(query: str, subreddit: str = "de") -> str:
        """Build search URL."""
        return f"https://old.reddit.com/r/{subreddit}/search?q={quote(query)}&restrict_sr=on&sort=relevance"


# German subreddits with ADAC content
GERMAN_SUBREDDITS = [
    "de",
    "germany",
    "finanzen",
    "de_IAmA",
]

# ADAC search queries
ADAC_QUERIES = [
    "ADAC",
    "ADAC Erfahrung",
    "ADAC Pannenhilfe",
    "ADAC Alternative",
    "Automobilclub",
]
