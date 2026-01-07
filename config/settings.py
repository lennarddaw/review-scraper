"""Application settings loaded from environment variables."""

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings with environment variable support."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # General
    environment: Literal["development", "production"] = "development"
    debug: bool = True
    
    # Project paths
    base_dir: Path = Field(default_factory=lambda: Path(__file__).parent.parent)
    
    @property
    def output_dir(self) -> Path:
        return self.base_dir / "data" / "exports"
    
    @property
    def raw_data_dir(self) -> Path:
        return self.base_dir / "data" / "raw"
    
    @property
    def checkpoint_dir(self) -> Path:
        return self.base_dir / "data" / "checkpoints"
    
    @property
    def log_dir(self) -> Path:
        return self.base_dir / "logs"

    # Scraping settings
    max_concurrent_requests: int = 5
    min_request_delay: float = 2.0
    max_request_delay: float = 5.0
    request_timeout: int = 30
    max_retries: int = 3

    # Rate limiting
    rate_limit_rpm: int = 20  # Requests per minute per domain

    # Proxy settings
    use_proxy: bool = False
    proxy_file: str = "proxies/proxies.txt"

    # Logging
    log_level: str = "INFO"
    log_file: str = "logs/scraper.log"

    # Browser automation
    browser_headless: bool = True
    browser_type: Literal["chromium", "firefox", "webkit"] = "chromium"

    def ensure_directories(self) -> None:
        """Create necessary directories if they don't exist."""
        for directory in [
            self.output_dir,
            self.raw_data_dir,
            self.checkpoint_dir,
            self.log_dir,
        ]:
            directory.mkdir(parents=True, exist_ok=True)


# Global settings instance
settings = Settings()