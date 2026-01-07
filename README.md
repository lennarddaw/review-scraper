# Review Scraper

A modular Python web scraper for collecting customer reviews from multiple sources. Designed for building AI training datasets.

## Features

- **Multi-source support**: Trustpilot, Sitejabber, IMDB, Steam, and more
- **Async-first**: High-performance concurrent scraping with httpx
- **Anti-detection**: User-agent rotation, realistic headers, human-like delays
- **Incremental saves**: Checkpoint/resume support for long scrapes
- **Rate limiting**: Per-domain rate limits to avoid bans
- **Clean output**: Exports to `{id, text}` JSON format for AI training

## Installation

```bash
# Clone/extract the project
cd review-scraper

# Create virtual environment
python -m venv venv
venv\Scripts\activate  # Windows
# source venv/bin/activate  # Linux/Mac

# Install dependencies
pip install -e .

# For browser automation (optional)
playwright install chromium
```

## Quick Start

### Scrape Trustpilot Reviews

```bash
# Scrape a specific company
python -m cli.main scrape trustpilot --url https://www.trustpilot.com/review/amazon.com --max 500

# Output: data/exports/trustpilot_reviews.json
```

### Scrape Steam Game Reviews

```bash
# By app ID
python -m cli.main scrape steam --url 1245620 --max 1000

# By URL
python -m cli.main scrape steam --url https://store.steampowered.com/app/1245620/ELDEN_RING/
```

### Scrape IMDB Movie Reviews

```bash
# By title ID
python -m cli.main scrape imdb --url tt0111161 --max 500
```

## CLI Commands

```bash
# Scrape reviews
python -m cli.main scrape <source> --url <url> [--max N] [--pages N] [--output file.json]

# List available sources
python -m cli.main sources

# Export to training format
python -m cli.main export input.json output.json --format training

# Show configuration
python -m cli.main info
```

## Output Format

Reviews are exported in the training data format:

```json
[
  {"id": 1, "text": "Great product, fast shipping..."},
  {"id": 2, "text": "Terrible quality, broke after one week..."},
  ...
]
```

Use `--metadata` flag to include full metadata:

```json
[
  {
    "id": 1,
    "text": "Great product...",
    "source": "trustpilot",
    "rating": 5.0,
    "date": "2024-01-15T10:30:00",
    "author": "John D."
  }
]
```

## Configuration

Copy `.env.example` to `.env` and configure:

```ini
# Scraping settings
MAX_CONCURRENT_REQUESTS=5
REQUEST_DELAY_MIN=2.0
REQUEST_DELAY_MAX=5.0
RATE_LIMIT_RPM=20

# Output
OUTPUT_DIR=data/exports
```

## Project Structure

```
review-scraper/
├── cli/                    # Command-line interface
│   └── main.py
├── config/                 # Configuration
│   ├── settings.py
│   ├── logging_config.py
│   └── sources.yaml
├── src/
│   ├── core/              # Core scraping engine
│   │   ├── base_scraper.py
│   │   ├── http_client.py
│   │   ├── rate_limiter.py
│   │   └── retry_handler.py
│   ├── scrapers/          # Site-specific scrapers
│   │   ├── review_platforms/
│   │   │   ├── trustpilot.py
│   │   │   └── sitejabber.py
│   │   ├── entertainment/
│   │   │   └── imdb.py
│   │   └── apps/
│   │       └── steam.py
│   ├── parsers/           # HTML/JSON parsing
│   ├── pipeline/          # Data cleaning
│   ├── storage/           # Export/persistence
│   └── antibot/           # Anti-detection
├── data/                  # Output directory
├── pyproject.toml
└── README.md
```

## Adding New Scrapers

Create a new scraper by extending `BaseScraper`:

```python
from src.core.base_scraper import BaseScraper
from src.models.review import Review, ReviewFactory

class MyNewScraper(BaseScraper):
    name = "mysite"
    base_url = "https://mysite.com"
    rate_limit_rpm = 15
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._review_factory = ReviewFactory()
    
    async def scrape_reviews(self, url: str, max_reviews: int = None) -> list[Review]:
        html = await self.http_client.get_text(url)
        # Parse and return reviews
        ...
    
    async def get_pagination_urls(self, base_url: str, max_pages: int = None) -> list[str]:
        # Return list of page URLs
        ...
    
    def parse_review_element(self, element) -> Review | None:
        # Parse single review from HTML element
        ...
```

## Supported Sources

| Source | Type | Difficulty | Notes |
|--------|------|------------|-------|
| Trustpilot | Reviews | Easy | Clean HTML, no JS required |
| Sitejabber | Reviews | Easy | Similar to Trustpilot |
| IMDB | Movies | Easy | User reviews, massive volume |
| Steam | Games | Easy | JSON API, very reliable |
| Google Play | Apps | Medium | Requires browser |
| Amazon | E-commerce | Hard | Anti-bot measures |

## Tips for Effective Scraping

1. **Start slow**: Use conservative rate limits initially
2. **Use delays**: Random delays between 2-5 seconds
3. **Rotate user agents**: Built-in rotation helps avoid detection
4. **Save incrementally**: Use checkpoint feature for long scrapes
5. **Respect robots.txt**: Be a good citizen

## License

MIT License