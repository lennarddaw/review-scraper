"""Sitejabber review scraper."""

import re
from datetime import datetime
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, Tag
from loguru import logger

from src.core.base_scraper import BaseScraper
from src.models.review import Review, ReviewFactory


class SitejabberScraper(BaseScraper):
    """
    Scraper for Sitejabber reviews.
    
    URL format: https://www.sitejabber.com/reviews/{domain}
    Pagination: ?page=N
    """

    name = "sitejabber"
    base_url = "https://www.sitejabber.com"
    rate_limit_rpm = 15
    requires_browser = False

    SELECTORS = {
        "review_container": "div.review",
        "review_text": "div.review__body",
        "review_title": "h3.review__title",
        "rating": "div.review__rating",
        "date": "time.review__date",
        "author": "span.review__author-name",
        "pagination_next": "a.pagination__next",
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._review_factory = ReviewFactory()

    async def scrape_reviews(self, url: str, max_reviews: int | None = None) -> list[Review]:
        """Scrape reviews from a Sitejabber page."""
        try:
            html = await self.http_client.get_text(url)
            return self._parse_reviews(html, url)
        except Exception as e:
            logger.error(f"[{self.name}] Error scraping {url}: {e}")
            return []

    def _parse_reviews(self, html: str, source_url: str) -> list[Review]:
        """Parse reviews from HTML content."""
        soup = BeautifulSoup(html, "lxml")
        reviews = []

        containers = soup.select(self.SELECTORS["review_container"])
        logger.debug(f"[{self.name}] Found {len(containers)} review containers")

        for container in containers:
            review = self.parse_review_element(container)
            if review:
                review.source_url = source_url
                reviews.append(review)

        return reviews

    def parse_review_element(self, element: Tag) -> Review | None:
        """Parse a single review element."""
        try:
            # Extract review text
            text_el = element.select_one(self.SELECTORS["review_text"])
            if not text_el:
                return None

            text = text_el.get_text(strip=True)
            if not text:
                return None

            # Extract title
            title_el = element.select_one(self.SELECTORS["review_title"])
            title = title_el.get_text(strip=True) if title_el else None

            # Combine
            full_text = f"{title}\n\n{text}" if title else text

            # Extract rating (from stars)
            rating = None
            rating_el = element.select_one(self.SELECTORS["rating"])
            if rating_el:
                # Count filled stars
                stars = rating_el.select("i.star--filled, i.icon-star")
                rating = float(len(stars)) if stars else None

            # Extract date
            date = None
            date_el = element.select_one(self.SELECTORS["date"])
            if date_el:
                datetime_str = date_el.get("datetime", "")
                if datetime_str:
                    try:
                        date = datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))
                    except ValueError:
                        pass

            # Extract author
            author = None
            author_el = element.select_one(self.SELECTORS["author"])
            if author_el:
                author = author_el.get_text(strip=True)

            return self._review_factory.create(
                text=full_text,
                source=self.name,
                title=title,
                rating=rating,
                date=date,
                author=author,
            )

        except Exception as e:
            logger.warning(f"[{self.name}] Error parsing review: {e}")
            return None

    async def get_pagination_urls(self, base_url: str, max_pages: int | None = None) -> list[str]:
        """Get pagination URLs."""
        urls = [base_url]
        max_pages = max_pages or 50  # Default limit

        current_url = base_url
        page = 1

        while page < max_pages:
            try:
                html = await self.http_client.get_text(current_url)
                soup = BeautifulSoup(html, "lxml")

                next_link = soup.select_one(self.SELECTORS["pagination_next"])
                if not next_link:
                    break

                next_url = next_link.get("href")
                if not next_url:
                    break

                next_url = urljoin(self.base_url, next_url)
                if next_url in urls:
                    break

                urls.append(next_url)
                current_url = next_url
                page += 1

            except Exception as e:
                logger.error(f"[{self.name}] Pagination error: {e}")
                break

        return urls

    @staticmethod
    def build_url(domain: str) -> str:
        """Build Sitejabber URL from domain."""
        domain = domain.lower().strip()
        domain = re.sub(r"^https?://", "", domain)
        domain = re.sub(r"^www\.", "", domain)
        domain = domain.rstrip("/")
        return f"https://www.sitejabber.com/reviews/{domain}"
