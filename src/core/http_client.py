"""Async HTTP client with retry logic, proxy support, and rate limiting."""

import asyncio
from typing import Any

import httpx
from loguru import logger

from config.settings import settings
from src.antibot.headers import HeaderGenerator
from src.antibot.delays import DelayManager
from src.core.rate_limiter import RateLimiter
from src.core.retry_handler import RetryHandler


class HttpClient:
    """Async HTTP client with built-in anti-detection features."""

    def __init__(
        self,
        rate_limiter: RateLimiter | None = None,
        proxy: str | None = None,
        timeout: int | None = None,
    ):
        self.timeout = timeout or settings.request_timeout
        self.proxy = proxy
        self.rate_limiter = rate_limiter or RateLimiter()
        self.retry_handler = RetryHandler()
        self.header_generator = HeaderGenerator()
        self.delay_manager = DelayManager()
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "HttpClient":
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()

    async def start(self) -> None:
        """Initialize the HTTP client."""
        transport = None
        if self.proxy:
            transport = httpx.AsyncHTTPTransport(proxy=self.proxy)

        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout),
            transport=transport,
            follow_redirects=True,
            http2=True,
        )
        logger.debug("HTTP client initialized")

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
            logger.debug("HTTP client closed")

    async def get(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        domain: str | None = None,
    ) -> httpx.Response:
        """
        Perform a GET request with rate limiting and retry logic.

        Args:
            url: The URL to fetch
            headers: Optional custom headers (merged with generated headers)
            params: Optional query parameters
            domain: Domain for rate limiting (extracted from URL if not provided)

        Returns:
            httpx.Response object
        """
        if not self._client:
            await self.start()

        # Extract domain for rate limiting
        if not domain:
            from urllib.parse import urlparse
            domain = urlparse(url).netloc

        # Apply rate limiting
        await self.rate_limiter.acquire(domain)

        # Generate headers
        request_headers = self.header_generator.generate()
        if headers:
            request_headers.update(headers)

        # Perform request with retry logic
        response = await self.retry_handler.execute(
            self._make_request,
            url=url,
            headers=request_headers,
            params=params,
        )

        # Apply delay after request
        await self.delay_manager.wait()

        return response

    async def _make_request(
        self,
        url: str,
        headers: dict[str, str],
        params: dict[str, Any] | None = None,
    ) -> httpx.Response:
        """Make the actual HTTP request."""
        logger.debug(f"Fetching: {url}")
        response = await self._client.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response

    async def get_text(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
    ) -> str:
        """Fetch URL and return response text."""
        response = await self.get(url, headers=headers, params=params)
        return response.text

    async def get_json(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Fetch URL and return JSON response."""
        response = await self.get(url, headers=headers, params=params)
        return response.json()


class HttpClientPool:
    """Pool of HTTP clients for concurrent scraping."""

    def __init__(self, size: int | None = None):
        self.size = size or settings.max_concurrent_requests
        self._semaphore = asyncio.Semaphore(self.size)
        self._clients: list[HttpClient] = []

    async def __aenter__(self) -> "HttpClientPool":
        """Initialize the client pool."""
        for _ in range(self.size):
            client = HttpClient()
            await client.start()
            self._clients.append(client)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close all clients in the pool."""
        for client in self._clients:
            await client.close()
        self._clients.clear()

    async def get(self, url: str, **kwargs) -> httpx.Response:
        """Get a client from the pool and make a request."""
        async with self._semaphore:
            client = self._clients[0]  # Simple round-robin could be added
            return await client.get(url, **kwargs)