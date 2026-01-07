"""User agent rotation for anti-detection."""

import random
from typing import Literal


# Latest user agents for major browsers (updated regularly)
USER_AGENTS = {
    "chrome_windows": [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ],
    "chrome_mac": [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    ],
    "firefox_windows": [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    ],
    "firefox_mac": [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0",
    ],
    "safari_mac": [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    ],
    "edge_windows": [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
    ],
}

# Flattened list of all user agents
ALL_USER_AGENTS = [ua for uas in USER_AGENTS.values() for ua in uas]


class UserAgentRotator:
    """Rotates user agents to avoid detection."""

    def __init__(
        self,
        browser: Literal["chrome", "firefox", "safari", "edge", "any"] = "any",
        platform: Literal["windows", "mac", "any"] = "any",
    ):
        """
        Initialize the rotator.

        Args:
            browser: Preferred browser type
            platform: Preferred platform
        """
        self.browser = browser
        self.platform = platform
        self._user_agents = self._filter_user_agents()
        self._index = 0

    def _filter_user_agents(self) -> list[str]:
        """Filter user agents based on preferences."""
        if self.browser == "any" and self.platform == "any":
            return ALL_USER_AGENTS.copy()

        filtered = []
        for key, agents in USER_AGENTS.items():
            browser_match = self.browser == "any" or key.startswith(self.browser)
            platform_match = self.platform == "any" or key.endswith(self.platform)
            
            if browser_match and platform_match:
                filtered.extend(agents)

        return filtered if filtered else ALL_USER_AGENTS.copy()

    def get_random(self) -> str:
        """Get a random user agent."""
        return random.choice(self._user_agents)

    def get_next(self) -> str:
        """Get the next user agent in rotation."""
        ua = self._user_agents[self._index % len(self._user_agents)]
        self._index += 1
        return ua

    def get_all(self) -> list[str]:
        """Get all available user agents."""
        return self._user_agents.copy()


def get_random_user_agent() -> str:
    """Quick helper to get a random user agent."""
    return random.choice(ALL_USER_AGENTS)