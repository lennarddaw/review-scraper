"""Abstract base class for all scrapers."""

from abc import ABC, abstractmethod
from typing import AsyncIterator
from urllib.parse import urlparse

from loguru import logger

from src.core.http_client import HttpClient
from src.core.rate_limiter import RateLimiter
from src.models.review import Review


class BaseScraper(ABC):
    """
    Abstract base class that all site-specific scrapers must implement.
    
    Provides common functionality like HTTP client management, pagination,
    and error handling. Subclasses must implement the abstract methods
    to handle site-specific parsing logic.
    """

    # Class-level configuration (override in subclasses)
    name: str = "base"
    base_url: str = ""
    rate_limit_rpm: int = 20
    requires_browser: bool = False

    def __init__(
        self,
        http_client: HttpClient | None = None,
        rate_limiter: RateLimiter | None = None,
    ):
        self.rate_limiter = rate_limiter or RateLimiter(self.rate_limit_rpm)
        self._http_client = http_client
        self._owns_client = http_client is None

    async def __aenter__(self) -> "BaseScraper":
        """Async context manager entry."""
        if self._owns_client:
            self._http_client = HttpClient(rate_limiter=self.rate_limiter)
            await self._http_client.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        if self._owns_client and self._http_client:
            await self._http_client.close()

    @property
    def http_client(self) -> HttpClient:
        """Get the HTTP client instance."""
        if not self._http_client:
            raise RuntimeError("HTTP client not initialized. Use 'async with' context manager.")
        return self._http_client

    @abstractmethod
    async def scrape_reviews(self, url: str, max_reviews: int | None = None) -> list[Review]:
        """
        Scrape reviews from a given URL.

        Args:
            url: The URL to scrape reviews from
            max_reviews: Maximum number of reviews to collect (None for all)

        Returns:
            List of Review objects
        """
        pass

    @abstractmethod
    async def get_pagination_urls(self, base_url: str, max_pages: int | None = None) -> list[str]:
        """
        Get all pagination URLs for a review page.

        Args:
            base_url: The starting URL
            max_pages: Maximum number of pages to collect (None for all)

        Returns:
            List of URLs for all review pages
        """
        pass

    @abstractmethod
    def parse_review_element(self, element) -> Review | None:
        """
        Parse a single review from an HTML element.

        Args:
            element: BeautifulSoup element containing a review

        Returns:
            Review object or None if parsing failed
        """
        pass

    async def scrape_all_pages(
        self,
        url: str,
        max_pages: int | None = None,
        max_reviews: int | None = None,
    ) -> AsyncIterator[Review]:
        """
        Scrape all pages of reviews from a URL.

        Args:
            url: The starting URL
            max_pages: Maximum number of pages to scrape
            max_reviews: Maximum total reviews to collect

        Yields:
            Review objects as they are scraped
        """
        logger.info(f"[{self.name}] Starting scrape of {url}")
        
        pages = await self.get_pagination_urls(url, max_pages)
        logger.info(f"[{self.name}] Found {len(pages)} pages to scrape")
        
        total_reviews = 0
        
        for page_num, page_url in enumerate(pages, 1):
            try:
                logger.debug(f"[{self.name}] Scraping page {page_num}/{len(pages)}: {page_url}")
                reviews = await self.scrape_reviews(page_url)
                
                for review in reviews:
                    yield review
                    total_reviews += 1
                    
                    if max_reviews and total_reviews >= max_reviews:
                        logger.info(f"[{self.name}] Reached max reviews limit ({max_reviews})")
                        return

                logger.info(f"[{self.name}] Page {page_num}: {len(reviews)} reviews (total: {total_reviews})")

            except Exception as e:
                logger.error(f"[{self.name}] Error scraping page {page_url}: {e}")
                continue

        logger.info(f"[{self.name}] Completed. Total reviews: {total_reviews}")

    async def scrape_url_list(
        self,
        urls: list[str],
        max_reviews_per_url: int | None = None,
    ) -> AsyncIterator[Review]:
        """
        Scrape reviews from a list of URLs.

        Args:
            urls: List of URLs to scrape
            max_reviews_per_url: Maximum reviews per URL

        Yields:
            Review objects as they are scraped
        """
        for url in urls:
            async for review in self.scrape_all_pages(url, max_reviews=max_reviews_per_url):
                yield review

    def get_domain(self, url: str) -> str:
        """Extract domain from URL."""
        return urlparse(url).netloc

    def build_url(self, path: str) -> str:
        """Build a full URL from a relative path."""
        if path.startswith("http"):
            return path
        base = self.base_url.rstrip("/")
        path = path.lstrip("/")
        return f"{base}/{path}"


class BrowserScraper(BaseScraper):
    """
    Base class for scrapers that require browser automation.
    
    Uses Playwright for rendering JavaScript-heavy pages.
    """

    requires_browser: bool = True

    def __init__(self, headless: bool = True, **kwargs):
        super().__init__(**kwargs)
        self.headless = headless
        self._browser = None
        self._context = None
        self._page = None

    async def __aenter__(self) -> "BrowserScraper":
        """Initialize browser."""
        await super().__aenter__()
        await self._init_browser()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close browser."""
        await self._close_browser()
        await super().__aexit__(exc_type, exc_val, exc_tb)

    async def _init_browser(self) -> None:
        """Initialize Playwright browser."""
        try:
            from playwright.async_api import async_playwright
            
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(headless=self.headless)
            self._context = await self._browser.new_context(
                user_agent=self._get_user_agent(),
                viewport={"width": 1920, "height": 1080},
            )
            self._page = await self._context.new_page()
            logger.debug(f"[{self.name}] Browser initialized")
        except ImportError:
            logger.error("Playwright not installed. Run: pip install playwright && playwright install")
            raise

    async def _close_browser(self) -> None:
        """Close Playwright browser."""
        if self._page:
            await self._page.close()
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if hasattr(self, "_playwright"):
            await self._playwright.stop()
        logger.debug(f"[{self.name}] Browser closed")

    def _get_user_agent(self) -> str:
        """Get a user agent string."""
        from src.antibot.user_agents import UserAgentRotator
        return UserAgentRotator().get_random()

    async def get_page_content(self, url: str, wait_selector: str | None = None) -> str:
        """
        Navigate to URL and get page content.

        Args:
            url: URL to navigate to
            wait_selector: CSS selector to wait for before getting content

        Returns:
            Page HTML content
        """
        await self._page.goto(url, wait_until="networkidle")
        
        if wait_selector:
            await self._page.wait_for_selector(wait_selector, timeout=10000)
        
        return await self._page.content()

    async def scroll_to_load(self, scroll_count: int = 5, delay: float = 1.0) -> None:
        """Scroll the page to trigger lazy loading."""
        import asyncio
        
        for _ in range(scroll_count):
            await self._page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(delay)