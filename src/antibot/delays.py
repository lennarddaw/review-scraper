"""Human-like delay patterns for anti-detection."""

import asyncio
import random
import time
from typing import Literal

from config.settings import settings


class DelayManager:
    """
    Manages delays between requests to appear more human-like.
    
    Supports various delay patterns:
    - Fixed: Constant delay
    - Random: Uniform random delay
    - Gaussian: Normal distribution around mean
    - Exponential: Exponentially distributed delays
    - Human: Mimics human browsing patterns
    """
    
    def __init__(
        self,
        min_delay: float | None = None,
        max_delay: float | None = None,
        pattern: Literal["fixed", "random", "gaussian", "exponential", "human"] = "random",
        enabled: bool = True,
    ):
        """
        Initialize delay manager.
        
        Args:
            min_delay: Minimum delay in seconds
            max_delay: Maximum delay in seconds
            pattern: Delay pattern to use
            enabled: Whether delays are enabled
        """
        self.min_delay = min_delay or settings.min_request_delay
        self.max_delay = max_delay or settings.max_request_delay
        self.pattern = pattern
        self.enabled = enabled
        
        # For human pattern
        self._request_count = 0
        self._session_start = time.time()
        
        # Statistics
        self.total_delay = 0.0
        self.delay_count = 0
    
    def get_delay(self) -> float:
        """
        Calculate delay based on pattern.
        
        Returns:
            Delay in seconds
        """
        if not self.enabled:
            return 0.0
        
        if self.pattern == "fixed":
            delay = self.min_delay
        
        elif self.pattern == "random":
            delay = random.uniform(self.min_delay, self.max_delay)
        
        elif self.pattern == "gaussian":
            # Normal distribution centered between min and max
            mean = (self.min_delay + self.max_delay) / 2
            std = (self.max_delay - self.min_delay) / 4
            delay = random.gauss(mean, std)
            delay = max(self.min_delay, min(self.max_delay, delay))
        
        elif self.pattern == "exponential":
            # Exponential distribution with lambda based on mean
            mean = (self.min_delay + self.max_delay) / 2
            delay = random.expovariate(1 / mean)
            delay = max(self.min_delay, min(self.max_delay, delay))
        
        elif self.pattern == "human":
            delay = self._human_delay()
        
        else:
            delay = self.min_delay
        
        # Update statistics
        self.total_delay += delay
        self.delay_count += 1
        
        return delay
    
    def _human_delay(self) -> float:
        """
        Generate human-like delay.
        
        Considers:
        - Longer delays after many requests (fatigue)
        - Occasional long pauses (distraction)
        - Random micro-variations
        """
        self._request_count += 1
        
        # Base delay
        base = random.uniform(self.min_delay, self.max_delay)
        
        # Fatigue factor - longer delays after many requests
        fatigue = 1.0 + (self._request_count / 100) * 0.5
        
        # Session duration factor
        session_minutes = (time.time() - self._session_start) / 60
        session_factor = 1.0 + (session_minutes / 30) * 0.3
        
        # Occasional distraction (5% chance of 5-15 second pause)
        distraction = 0.0
        if random.random() < 0.05:
            distraction = random.uniform(5.0, 15.0)
        
        # Micro-variations
        jitter = random.gauss(0, 0.2)
        
        delay = base * fatigue * session_factor + distraction + jitter
        
        # Clamp to reasonable bounds
        return max(self.min_delay, min(self.max_delay * 3, delay))
    
    async def wait(self) -> float:
        """
        Wait for calculated delay.
        
        Returns:
            Actual delay waited
        """
        delay = self.get_delay()
        if delay > 0:
            await asyncio.sleep(delay)
        return delay
    
    def wait_sync(self) -> float:
        """
        Synchronous wait for calculated delay.
        
        Returns:
            Actual delay waited
        """
        delay = self.get_delay()
        if delay > 0:
            time.sleep(delay)
        return delay
    
    def reset_session(self) -> None:
        """Reset session tracking for human pattern."""
        self._request_count = 0
        self._session_start = time.time()
    
    @property
    def average_delay(self) -> float:
        """Get average delay."""
        if self.delay_count == 0:
            return 0.0
        return self.total_delay / self.delay_count
    
    def get_stats(self) -> dict[str, float]:
        """Get delay statistics."""
        return {
            "total_delay": self.total_delay,
            "delay_count": self.delay_count,
            "average_delay": self.average_delay,
            "request_count": self._request_count,
        }


async def human_delay(
    min_seconds: float = 1.0,
    max_seconds: float = 5.0,
) -> float:
    """
    Simple async human-like delay.
    
    Args:
        min_seconds: Minimum delay
        max_seconds: Maximum delay
        
    Returns:
        Actual delay
    """
    delay = random.uniform(min_seconds, max_seconds)
    
    # Add small random variation
    delay += random.gauss(0, 0.3)
    delay = max(min_seconds, min(max_seconds * 1.5, delay))
    
    await asyncio.sleep(delay)
    return delay


def human_delay_sync(
    min_seconds: float = 1.0,
    max_seconds: float = 5.0,
) -> float:
    """
    Simple sync human-like delay.
    
    Args:
        min_seconds: Minimum delay
        max_seconds: Maximum delay
        
    Returns:
        Actual delay
    """
    delay = random.uniform(min_seconds, max_seconds)
    delay += random.gauss(0, 0.3)
    delay = max(min_seconds, min(max_seconds * 1.5, delay))
    
    time.sleep(delay)
    return delay


class AdaptiveDelayManager(DelayManager):
    """
    Delay manager that adapts based on response patterns.
    
    Increases delays when detecting rate limiting,
    decreases when responses are fast.
    """
    
    def __init__(
        self,
        min_delay: float = 1.0,
        max_delay: float = 10.0,
        target_delay: float = 2.0,
        adaptation_rate: float = 0.1,
    ):
        """
        Initialize adaptive delay manager.
        
        Args:
            min_delay: Minimum possible delay
            max_delay: Maximum possible delay
            target_delay: Target delay to adapt around
            adaptation_rate: How fast to adapt (0-1)
        """
        super().__init__(min_delay, max_delay, pattern="random")
        self.target_delay = target_delay
        self.adaptation_rate = adaptation_rate
        self._current_delay = target_delay
    
    def report_success(self, response_time: float) -> None:
        """
        Report a successful request.
        
        Args:
            response_time: Response time in seconds
        """
        # Fast response = can decrease delay slightly
        if response_time < 1.0:
            adjustment = -self.adaptation_rate
        else:
            adjustment = 0
        
        self._current_delay = max(
            self.min_delay,
            min(self.max_delay, self._current_delay + adjustment)
        )
    
    def report_rate_limited(self) -> None:
        """Report a rate-limited response (429)."""
        # Significantly increase delay
        self._current_delay = min(
            self.max_delay,
            self._current_delay * 2
        )
    
    def report_error(self) -> None:
        """Report a request error."""
        # Slightly increase delay
        self._current_delay = min(
            self.max_delay,
            self._current_delay * 1.2
        )
    
    def get_delay(self) -> float:
        """Get adaptive delay."""
        if not self.enabled:
            return 0.0
        
        # Add some randomness around current delay
        delay = random.gauss(self._current_delay, self._current_delay * 0.2)
        delay = max(self.min_delay, min(self.max_delay, delay))
        
        self.total_delay += delay
        self.delay_count += 1
        
        return delay


class PageDelayManager:
    """
    Manages delays specific to page navigation patterns.
    
    Applies different delays for different page types.
    """
    
    def __init__(self):
        """Initialize page delay manager."""
        self._page_delays = {
            "search": (2.0, 5.0),      # Search results pages
            "listing": (1.5, 4.0),     # Product/review listing pages
            "detail": (3.0, 8.0),      # Detail pages (longer reading)
            "pagination": (1.0, 3.0),  # Pagination clicks
            "ajax": (0.5, 2.0),        # AJAX requests
            "default": (1.0, 5.0),     # Default
        }
    
    def set_delay(self, page_type: str, min_delay: float, max_delay: float) -> None:
        """Set delay range for a page type."""
        self._page_delays[page_type] = (min_delay, max_delay)
    
    async def wait(self, page_type: str = "default") -> float:
        """
        Wait with delay appropriate for page type.
        
        Args:
            page_type: Type of page
            
        Returns:
            Actual delay
        """
        min_d, max_d = self._page_delays.get(
            page_type,
            self._page_delays["default"]
        )
        
        delay = random.uniform(min_d, max_d)
        await asyncio.sleep(delay)
        return delay