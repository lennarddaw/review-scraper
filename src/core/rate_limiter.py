"""Rate limiter for controlling request frequency per domain."""

import asyncio
from collections import defaultdict
from time import time

from aiolimiter import AsyncLimiter
from loguru import logger

from config.settings import settings


class RateLimiter:
    """
    Per-domain rate limiter using token bucket algorithm.
    
    Ensures we don't exceed the configured requests per minute for each domain.
    """

    def __init__(self, requests_per_minute: int | None = None):
        self.rpm = requests_per_minute or settings.rate_limit_rpm
        self._limiters: dict[str, AsyncLimiter] = {}
        self._request_counts: dict[str, int] = defaultdict(int)
        self._lock = asyncio.Lock()

    def _get_limiter(self, domain: str) -> AsyncLimiter:
        """Get or create a rate limiter for the given domain."""
        if domain not in self._limiters:
            # Convert RPM to rate per second
            # AsyncLimiter(max_rate, time_period) - max_rate tokens per time_period seconds
            self._limiters[domain] = AsyncLimiter(self.rpm, 60)
            logger.debug(f"Created rate limiter for {domain}: {self.rpm} rpm")
        return self._limiters[domain]

    async def acquire(self, domain: str) -> None:
        """
        Acquire permission to make a request to the given domain.
        
        Blocks until a request slot is available.
        """
        limiter = self._get_limiter(domain)
        async with limiter:
            async with self._lock:
                self._request_counts[domain] += 1
            logger.debug(f"Rate limit acquired for {domain} (total: {self._request_counts[domain]})")

    def get_stats(self) -> dict[str, int]:
        """Get request counts per domain."""
        return dict(self._request_counts)

    def reset_stats(self) -> None:
        """Reset request counts."""
        self._request_counts.clear()


class AdaptiveRateLimiter(RateLimiter):
    """
    Adaptive rate limiter that adjusts based on response patterns.
    
    Slows down when detecting potential blocking, speeds up when stable.
    """

    def __init__(self, initial_rpm: int | None = None):
        super().__init__(initial_rpm)
        self._error_counts: dict[str, int] = defaultdict(int)
        self._success_counts: dict[str, int] = defaultdict(int)
        self._domain_rpm: dict[str, int] = {}
        self._last_adjustment: dict[str, float] = {}
        self._min_rpm = 5
        self._max_rpm = 60
        self._adjustment_interval = 60  # seconds

    async def record_success(self, domain: str) -> None:
        """Record a successful request."""
        async with self._lock:
            self._success_counts[domain] += 1
            await self._maybe_adjust(domain)

    async def record_error(self, domain: str, status_code: int) -> None:
        """Record a failed request."""
        async with self._lock:
            self._error_counts[domain] += 1
            
            # Immediately slow down on rate limit errors
            if status_code in (429, 503):
                await self._slow_down(domain)

    async def _maybe_adjust(self, domain: str) -> None:
        """Check if we should adjust the rate limit."""
        now = time()
        last = self._last_adjustment.get(domain, 0)
        
        if now - last < self._adjustment_interval:
            return
        
        self._last_adjustment[domain] = now
        
        success = self._success_counts[domain]
        errors = self._error_counts[domain]
        
        if errors == 0 and success > 10:
            await self._speed_up(domain)
        elif errors > 0:
            error_rate = errors / (success + errors)
            if error_rate > 0.1:  # More than 10% errors
                await self._slow_down(domain)

    async def _slow_down(self, domain: str) -> None:
        """Reduce the rate limit for a domain."""
        current = self._domain_rpm.get(domain, self.rpm)
        new_rpm = max(self._min_rpm, int(current * 0.7))
        
        if new_rpm != current:
            self._domain_rpm[domain] = new_rpm
            self._limiters[domain] = AsyncLimiter(new_rpm, 60)
            logger.warning(f"Slowing down {domain}: {current} -> {new_rpm} rpm")

    async def _speed_up(self, domain: str) -> None:
        """Increase the rate limit for a domain."""
        current = self._domain_rpm.get(domain, self.rpm)
        new_rpm = min(self._max_rpm, int(current * 1.2))
        
        if new_rpm != current:
            self._domain_rpm[domain] = new_rpm
            self._limiters[domain] = AsyncLimiter(new_rpm, 60)
            logger.info(f"Speeding up {domain}: {current} -> {new_rpm} rpm")