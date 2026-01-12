"""
ADAC Multi-Source Review Scraper

Collects reviews from all available sources for ADAC:
- Trustpilot
- Kununu (employee reviews)
- Google Play Store (ADAC apps)
- Apple App Store (ADAC apps)
- ProvenExpert

Usage:
    python scripts/scrape_adac.py [--max-reviews 500]
"""

import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from loguru import logger
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

from config.settings import settings
from config.logging_config import setup_logging
from src.models.review import Review

console = Console()


# ADAC Sources Configuration
ADAC_SOURCES = {
    "trustpilot": {
        "scraper": "review_platforms.trustpilot",
        "urls": [
            "https://www.trustpilot.com/review/adac.de",
        ],
        "max_per_url": 2000,
    },
    "kununu": {
        "scraper": "review_platforms.kununu",
        "urls": [
            "https://www.kununu.com/de/adac",
        ],
        "max_per_url": 1000,
    },
    "google_play": {
        "scraper": "apps.google_play",
        "urls": [
            "de.adac.android",
            "de.adac.android.spritpreise",
            "de.adac.android.maps",
            "de.adac.camping",
        ],
        "max_per_url": 1000,
    },
    "app_store": {
        "scraper": "apps.app_store",
        "urls": [
            "adac:397267553",
            "adac-spritpreise:365469498",
            "adac-camping:397304488",
        ],
        "max_per_url": 500,
    },
    "provenexpert": {
        "scraper": "review_platforms.provenexpert",
        "urls": [
            "adac",
        ],
        "max_per_url": 200,
    },
}


async def scrape_source(source_name: str, config: dict, max_reviews: int | None = None) -> list[Review]:
    """Scrape reviews from a single source."""
    from src.scrapers.registry import ScraperRegistry
    
    reviews = []
    scraper_path = config["scraper"]
    
    # Load scraper
    scraper_cls = ScraperRegistry.load_from_module(scraper_path)
    if not scraper_cls:
        logger.error(f"Could not load scraper: {scraper_path}")
        return reviews
    
    max_per_url = max_reviews or config.get("max_per_url", 500)
    
    async with scraper_cls() as scraper:
        for url in config["urls"]:
            try:
                console.print(f"  → Scraping [cyan]{url}[/cyan]...")
                
                url_reviews = []
                async for review in scraper.scrape_all_pages(url, max_reviews=max_per_url):
                    url_reviews.append(review)
                    
                    if max_reviews and len(reviews) + len(url_reviews) >= max_reviews:
                        break
                
                reviews.extend(url_reviews)
                console.print(f"    ✓ Got {len(url_reviews)} reviews")
                
                if max_reviews and len(reviews) >= max_reviews:
                    break
                    
            except Exception as e:
                logger.error(f"Error scraping {url}: {e}")
                console.print(f"    ✗ Error: {e}", style="red")
    
    return reviews


async def scrape_all_adac(max_reviews_per_source: int | None = None) -> dict[str, list[Review]]:
    """Scrape all ADAC sources."""
    results = {}
    
    console.print("\n[bold blue]ADAC Multi-Source Review Scraper[/bold blue]")
    console.print("=" * 50)
    
    for source_name, config in ADAC_SOURCES.items():
        console.print(f"\n[bold green]► {source_name.upper()}[/bold green]")
        
        try:
            reviews = await scrape_source(source_name, config, max_reviews_per_source)
            results[source_name] = reviews
            console.print(f"  [bold]Total: {len(reviews)} reviews[/bold]")
        except Exception as e:
            logger.error(f"Failed to scrape {source_name}: {e}")
            console.print(f"  [red]Failed: {e}[/red]")
            results[source_name] = []
    
    return results


def save_results(results: dict[str, list[Review]], output_dir: Path) -> dict[str, Path]:
    """Save results to JSON files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {}
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save per-source files
    for source_name, reviews in results.items():
        if reviews:
            filename = f"adac_{source_name}_{timestamp}.json"
            filepath = output_dir / filename
            
            data = [r.to_export_dict() for r in reviews]
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            paths[source_name] = filepath
    
    # Save combined file
    all_reviews = []
    for reviews in results.values():
        all_reviews.extend(reviews)
    
    if all_reviews:
        # Reassign IDs
        for i, review in enumerate(all_reviews, 1):
            review.id = i
        
        combined_path = output_dir / f"adac_all_{timestamp}.json"
        data = [r.to_export_dict() for r in all_reviews]
        with open(combined_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        paths["_combined"] = combined_path
    
    return paths


def print_summary(results: dict[str, list[Review]], paths: dict[str, Path]):
    """Print scraping summary."""
    console.print("\n" + "=" * 50)
    console.print("[bold]SCRAPING SUMMARY[/bold]")
    console.print("=" * 50)
    
    table = Table(show_header=True, header_style="bold")
    table.add_column("Source")
    table.add_column("Reviews", justify="right")
    table.add_column("File")
    
    total = 0
    for source_name, reviews in results.items():
        count = len(reviews)
        total += count
        filepath = paths.get(source_name, "")
        table.add_row(
            source_name,
            str(count),
            str(filepath.name) if filepath else "-"
        )
    
    table.add_row("[bold]TOTAL[/bold]", f"[bold]{total}[/bold]", "")
    console.print(table)
    
    if "_combined" in paths:
        console.print(f"\n[green]✓ Combined file:[/green] {paths['_combined']}")


async def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Scrape ADAC reviews from all sources")
    parser.add_argument("--max-reviews", type=int, default=None,
                       help="Max reviews per source (default: source-specific)")
    parser.add_argument("--output-dir", type=str, default=None,
                       help="Output directory (default: data/exports)")
    args = parser.parse_args()
    
    setup_logging()
    settings.ensure_directories()
    
    output_dir = Path(args.output_dir) if args.output_dir else settings.output_dir
    
    # Run scraping
    results = await scrape_all_adac(args.max_reviews)
    
    # Save results
    paths = save_results(results, output_dir)
    
    # Print summary
    print_summary(results, paths)


if __name__ == "__main__":
    asyncio.run(main())
