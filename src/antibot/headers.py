"""HTTP header generation for realistic requests."""

import random
from src.antibot.user_agents import UserAgentRotator


class HeaderGenerator:
    """Generates realistic HTTP headers to avoid bot detection."""

    ACCEPT_HTML = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
    ACCEPT_JSON = "application/json, text/plain, */*"
    
    ACCEPT_LANGUAGES = [
        "en-US,en;q=0.9",
        "en-GB,en;q=0.9,en-US;q=0.8",
        "en-US,en;q=0.9,de;q=0.8",
    ]
    
    ACCEPT_ENCODING = "gzip, deflate, br"
    
    SEC_CH_UA = [
        '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        '"Chromium";v="121", "Not A(Brand";v="99", "Google Chrome";v="121"',
    ]

    def __init__(self, user_agent_rotator: UserAgentRotator | None = None):
        self.ua_rotator = user_agent_rotator or UserAgentRotator()

    def generate(
        self,
        referer: str | None = None,
        accept: str | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> dict[str, str]:
        """Generate a complete set of realistic headers."""
        headers = {
            "User-Agent": self.ua_rotator.get_random(),
            "Accept": accept or self.ACCEPT_HTML,
            "Accept-Language": random.choice(self.ACCEPT_LANGUAGES),
            "Accept-Encoding": self.ACCEPT_ENCODING,
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none" if not referer else "same-origin",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }

        if "Chrome" in headers["User-Agent"]:
            headers["Sec-Ch-Ua"] = random.choice(self.SEC_CH_UA)
            headers["Sec-Ch-Ua-Mobile"] = "?0"
            headers["Sec-Ch-Ua-Platform"] = '"Windows"'

        if referer:
            headers["Referer"] = referer

        if extra_headers:
            headers.update(extra_headers)

        return headers

    def generate_for_ajax(self, referer: str | None = None) -> dict[str, str]:
        """Generate headers for AJAX requests."""
        headers = self.generate(referer=referer, accept=self.ACCEPT_JSON)
        headers["X-Requested-With"] = "XMLHttpRequest"
        headers["Sec-Fetch-Dest"] = "empty"
        headers["Sec-Fetch-Mode"] = "cors"
        return headers