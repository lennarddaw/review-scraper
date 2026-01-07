"""Retry handler with exponential backoff for failed requests."""

import asyncio
from typing import Any, Callable, TypeVar

import httpx
from loguru import logger

from config.settings import settings

T = TypeVar("T")


class RetryHandler:
    """
    Handles retrying failed requests with exponential backoff.
    
    Retries on transient errors like network issues, rate limits, and server errors.
    """

    # Status codes that should trigger a retry
    RETRYABLE_STATUS_CODES = {
        408,  # Request Timeout
        429,  # Too Many Requests
        500,  # Internal Server Error
        502,  # Bad Gateway
        503,  # Service Unavailable
        504,  # Gateway Timeout
    }

    # Exceptions that should trigger a retry
    RETRYABLE_EXCEPTIONS = (
        httpx.TimeoutException,
        httpx.NetworkError,
        httpx.ConnectError,
        ConnectionError,
        asyncio.TimeoutError,
    )

    def __init__(
        self,
        max_retries: int | None = None,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
    ):
        self.max_retries = max_retries or settings.max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base

    async def execute(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """
        Execute a function with retry logic.

        Args:
            func: The async function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            The result of the function

        Raises:
            The last exception if all retries are exhausted
        """
        last_exception: Exception | None = None

        for attempt in range(self.max_retries + 1):
            try:
                result = await func(*args, **kwargs)
                
                # Check for retryable status codes
                if isinstance(result, httpx.Response):
                    if result.status_code in self.RETRYABLE_STATUS_CODES:
                        raise httpx.HTTPStatusError(
                            f"Retryable status code: {result.status_code}",
                            request=result.request,
                            response=result,
                        )
                
                return result

            except self.RETRYABLE_EXCEPTIONS as e:
                last_exception = e
                await self._handle_retry(attempt, e)

            except httpx.HTTPStatusError as e:
                if e.response.status_code in self.RETRYABLE_STATUS_CODES:
                    last_exception = e
                    await self._handle_retry(attempt, e)
                else:
                    # Non-retryable HTTP error
                    raise

        # All retries exhausted
        logger.error(f"All {self.max_retries} retries exhausted")
        raise last_exception or RuntimeError("Retry failed with no exception")

    async def _handle_retry(self, attempt: int, exception: Exception) -> None:
        """Handle a retry attempt."""
        if attempt >= self.max_retries:
            logger.error(f"Final retry attempt failed: {exception}")
            raise exception

        delay = self._calculate_delay(attempt)
        logger.warning(
            f"Attempt {attempt + 1}/{self.max_retries + 1} failed: {exception}. "
            f"Retrying in {delay:.2f}s..."
        )
        await asyncio.sleep(delay)

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate the delay for a retry attempt using exponential backoff."""
        delay = self.base_delay * (self.exponential_base ** attempt)
        # Add jitter (±25%)
        import random
        jitter = delay * 0.25 * (2 * random.random() - 1)
        delay = min(delay + jitter, self.max_delay)
        return max(0.1, delay)


class CircuitBreaker:
    """
    Circuit breaker pattern to prevent hammering failing services.
    
    States:
    - CLOSED: Normal operation, requests go through
    - OPEN: Too many failures, requests are rejected immediately
    - HALF_OPEN: Testing if service has recovered
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        success_threshold: int = 3,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        
        self._failures: dict[str, int] = {}
        self._successes: dict[str, int] = {}
        self._state: dict[str, str] = {}  # CLOSED, OPEN, HALF_OPEN
        self._last_failure_time: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def can_proceed(self, key: str) -> bool:
        """Check if a request can proceed."""
        async with self._lock:
            state = self._state.get(key, "CLOSED")
            
            if state == "CLOSED":
                return True
            
            if state == "OPEN":
                # Check if recovery timeout has passed
                import time
                if time.time() - self._last_failure_time.get(key, 0) > self.recovery_timeout:
                    self._state[key] = "HALF_OPEN"
                    self._successes[key] = 0
                    logger.info(f"Circuit breaker for {key} entering HALF_OPEN state")
                    return True
                return False
            
            # HALF_OPEN
            return True

    async def record_success(self, key: str) -> None:
        """Record a successful request."""
        async with self._lock:
            state = self._state.get(key, "CLOSED")
            
            if state == "HALF_OPEN":
                self._successes[key] = self._successes.get(key, 0) + 1
                if self._successes[key] >= self.success_threshold:
                    self._state[key] = "CLOSED"
                    self._failures[key] = 0
                    logger.info(f"Circuit breaker for {key} CLOSED after recovery")
            
            elif state == "CLOSED":
                # Reset failure count on success
                self._failures[key] = 0

    async def record_failure(self, key: str) -> None:
        """Record a failed request."""
        import time
        
        async with self._lock:
            self._failures[key] = self._failures.get(key, 0) + 1
            self._last_failure_time[key] = time.time()
            
            state = self._state.get(key, "CLOSED")
            
            if state == "HALF_OPEN":
                # Any failure in half-open goes back to open
                self._state[key] = "OPEN"
                logger.warning(f"Circuit breaker for {key} re-OPENED after failure in HALF_OPEN")
            
            elif state == "CLOSED" and self._failures[key] >= self.failure_threshold:
                self._state[key] = "OPEN"
                logger.warning(f"Circuit breaker for {key} OPENED after {self.failure_threshold} failures")