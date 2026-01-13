"""
ADAC Multi-Source Review Scraper - COMPREHENSIVE GERMAN FEEDBACK

Collects reviews from all available German sources for ADAC:

REVIEW PLATFORMS:
- Trustpilot (adac.de)
- Kununu (employee reviews)
- ProvenExpert (business reviews)

APP STORES:
- Google Play Store (ADAC apps)
- Apple App Store (ADAC apps)

FORUMS & Q&A:
- Gutefrage.net (German Q&A)
- Motor-Talk.de (Auto forum)
- Reddit (r/de, r/germany)

LOCAL/MAPS:
- Google Maps Reviews (100+ ADAC Standorte)

E-COMMERCE:
- Amazon.de (ADAC products)

Target: 15,000+ German feedbacks

Usage:
    python scripts/scrape_adac.py [--max-reviews 500]
    python scripts/scrape_adac.py --category local  # Only Google Maps
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


# ADAC Sources Configuration - Comprehensive German Feedback
ADAC_SOURCES = {
    # === REVIEW PLATFORMS ===
    "trustpilot": {
        "scraper": "review_platforms.trustpilot",
        "urls": [
            "https://www.trustpilot.com/review/adac.de",
        ],
        "max_per_url": 2000,
        "category": "reviews",
    },
    "kununu": {
        "scraper": "review_platforms.kununu",
        "urls": [
            "https://www.kununu.com/de/adac",
            "https://www.kununu.com/de/adac-ev",
        ],
        "max_per_url": 1000,
        "category": "reviews",
    },
    "provenexpert": {
        "scraper": "review_platforms.provenexpert",
        "urls": [
            "adac",
        ],
        "max_per_url": 200,
        "category": "reviews",
    },
    
    # === APP STORES ===
    "google_play": {
        "scraper": "apps.google_play",
        "urls": [
            "de.adac.android",
            "de.adac.android.spritpreise",
            "de.adac.android.maps",
            "de.adac.camping",
        ],
        "max_per_url": 1000,
        "category": "apps",
    },
    "app_store": {
        "scraper": "apps.app_store",
        "urls": [
            "adac:397267553",
            "adac-spritpreise:365469498",
            "adac-camping:397304488",
        ],
        "max_per_url": 500,
        "category": "apps",
    },
    
    # === FORUMS & Q&A ===
    "gutefrage": {
        "scraper": "forums.gutefrage",
        "urls": [
            "ADAC Erfahrungen",
            "ADAC Pannenhilfe",
            "ADAC Mitgliedschaft",
            "ADAC Plus Premium",
            "ADAC Versicherung",
            "ADAC Alternative",
            "ADAC Wartezeit",
            "ADAC Fahrsicherheitstraining",
            "ADAC App",
        ],
        "max_per_url": 200,
        "category": "forums",
    },
    "motor_talk": {
        "scraper": "forums.motor_talk",
        "urls": [
            "https://www.motor-talk.de/forum/eure-erfahrungen-mit-dem-adac-t564580.html",
            "https://www.motor-talk.de/forum/adac-pannendienst-wirklich-so-schlecht-t7068953.html",
            "https://www.motor-talk.de/forum/avd-oder-adac-t8256692.html",
            "https://www.motor-talk.de/forum/alternativen-zum-adac-gesucht-habt-ihr-erfahrungen-t4847103.html",
            "https://www.motor-talk.de/forum/adac-autoversicherung-erfahrung-t6498781.html",
            "ADAC Erfahrungen",
            "ADAC Pannenhilfe",
        ],
        "max_per_url": 100,
        "category": "forums",
    },
    "reddit": {
        "scraper": "forums.reddit",
        "urls": [
            "https://old.reddit.com/r/de/search?q=ADAC&restrict_sr=on&sort=relevance",
            "https://old.reddit.com/r/germany/search?q=ADAC&restrict_sr=on&sort=relevance",
            "https://old.reddit.com/r/finanzen/search?q=ADAC&restrict_sr=on&sort=relevance",
        ],
        "max_per_url": 200,
        "category": "forums",
    },
    
    # === E-COMMERCE ===
    "amazon_de": {
        "scraper": "ecommerce.amazon_de",
        "urls": [
            # ADAC Reiseführer und Bücher
            "3826422716",  # ADAC Reiseatlas
            "3986450483",  # ADAC Camping Guide
        ],
        "max_per_url": 100,
        "category": "ecommerce",
    },
    
    # === GOOGLE MAPS (Local Reviews) ===
    # Requires: pip install google-search-results
    # And SERPAPI_API_KEY environment variable (free tier: 100 searches/month)
    "google_maps": {
        "scraper": "maps.google_maps_serpapi",
        "urls": [
            # Major cities - Geschäftsstellen
            "ADAC Geschäftsstelle Berlin",
            "ADAC Geschäftsstelle Hamburg",
            "ADAC Geschäftsstelle München",
            "ADAC Geschäftsstelle Köln",
            "ADAC Geschäftsstelle Frankfurt",
            "ADAC Geschäftsstelle Stuttgart",
            "ADAC Geschäftsstelle Düsseldorf",
            "ADAC Geschäftsstelle Hannover",
            "ADAC Geschäftsstelle Leipzig",
            "ADAC Geschäftsstelle Dresden",
            "ADAC Geschäftsstelle Nürnberg",
            "ADAC Geschäftsstelle Bremen",
            "ADAC Geschäftsstelle Essen",
            "ADAC Geschäftsstelle Dortmund",
            # Fahrsicherheitszentren
            "ADAC Fahrsicherheitszentrum Linthe",
            "ADAC Fahrsicherheitszentrum Grevenbroich",
            "ADAC Fahrsicherheitszentrum Augsburg",
            "ADAC Fahrsicherheitszentrum Hannover",
            "ADAC Fahrsicherheitszentrum Koblenz",
            # More cities
            "ADAC Augsburg",
            "ADAC Bonn",
            "ADAC Karlsruhe",
            "ADAC Mannheim",
            "ADAC Wiesbaden",
            "ADAC Münster",
            "ADAC Freiburg",
            "ADAC Mainz",
            "ADAC Regensburg",
            "ADAC Würzburg",
        ],
        "max_per_url": 5000,
        "category": "local",
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
    
    try:
        async with scraper_cls() as scraper:
            for url in config["urls"]:
                try:
                    console.print(f"  → Scraping [cyan]{url[:60]}...[/cyan]")
                    
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
    except Exception as e:
        logger.error(f"Scraper initialization failed for {scraper_path}: {e}")
        console.print(f"  ✗ Scraper failed: {e}", style="red")
    
    return reviews


async def scrape_all_adac(max_reviews_per_source: int | None = None, categories: list[str] | None = None) -> dict[str, list[Review]]:
    """Scrape all ADAC sources."""
    results = {}
    
    console.print("\n[bold blue]═══════════════════════════════════════════════════[/bold blue]")
    console.print("[bold blue]  ADAC COMPREHENSIVE GERMAN FEEDBACK COLLECTOR[/bold blue]")
    console.print("[bold blue]═══════════════════════════════════════════════════[/bold blue]")
    console.print(f"\nTarget: [bold]10,000+[/bold] German feedbacks")
    console.print(f"Categories: {categories or 'ALL'}\n")
    
    for source_name, config in ADAC_SOURCES.items():
        # Filter by category if specified
        if categories and config.get("category") not in categories:
            continue
            
        category = config.get("category", "other")
        console.print(f"\n[bold green]► {source_name.upper()}[/bold green] [{category}]")
        
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
    
    # Save combined file in training format
    all_reviews = []
    for reviews in results.values():
        all_reviews.extend(reviews)
    
    if all_reviews:
        # Reassign IDs for training format
        training_data = []
        for i, review in enumerate(all_reviews, 1):
            training_data.append({
                "id": i,
                "text": review.text,
            })
        
        # Training format file
        training_path = output_dir / f"adac_training_{timestamp}.json"
        with open(training_path, 'w', encoding='utf-8') as f:
            json.dump(training_data, f, ensure_ascii=False, indent=2)
        paths["_training"] = training_path
        
        # Full data file
        combined_path = output_dir / f"adac_all_{timestamp}.json"
        full_data = [r.to_export_dict() for r in all_reviews]
        with open(combined_path, 'w', encoding='utf-8') as f:
            json.dump(full_data, f, ensure_ascii=False, indent=2)
        paths["_combined"] = combined_path
    
    return paths


def print_summary(results: dict[str, list[Review]], paths: dict[str, Path]):
    """Print scraping summary."""
    console.print("\n" + "═" * 55)
    console.print("[bold]SCRAPING SUMMARY - ADAC GERMAN FEEDBACK[/bold]")
    console.print("═" * 55)
    
    # Group by category
    categories = {}
    for source_name, reviews in results.items():
        config = ADAC_SOURCES.get(source_name, {})
        cat = config.get("category", "other")
        if cat not in categories:
            categories[cat] = {"sources": [], "total": 0}
        categories[cat]["sources"].append((source_name, len(reviews)))
        categories[cat]["total"] += len(reviews)
    
    total = 0
    for cat_name, cat_data in categories.items():
        console.print(f"\n[bold cyan]{cat_name.upper()}[/bold cyan]")
        table = Table(show_header=True, header_style="bold")
        table.add_column("Source")
        table.add_column("Reviews", justify="right")
        
        for source_name, count in cat_data["sources"]:
            table.add_row(source_name, str(count))
            total += count
        
        table.add_row("[bold]Subtotal[/bold]", f"[bold]{cat_data['total']}[/bold]")
        console.print(table)
    
    console.print(f"\n[bold green]═══ GRAND TOTAL: {total} REVIEWS ═══[/bold green]")
    
    if "_training" in paths:
        console.print(f"\n[green]✓ Training file:[/green] {paths['_training']}")
    if "_combined" in paths:
        console.print(f"[green]✓ Full data file:[/green] {paths['_combined']}")


async def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Scrape ADAC reviews from all German sources")
    parser.add_argument("--max-reviews", type=int, default=None,
                       help="Max reviews per source (default: source-specific)")
    parser.add_argument("--output-dir", type=str, default=None,
                       help="Output directory (default: data/exports)")
    parser.add_argument("--category", type=str, nargs="+", choices=["reviews", "apps", "forums", "ecommerce", "local"],
                       help="Only scrape specific categories")
    args = parser.parse_args()
    
    setup_logging()
    settings.ensure_directories()
    
    output_dir = Path(args.output_dir) if args.output_dir else settings.output_dir
    
    # Run scraping
    results = await scrape_all_adac(args.max_reviews, args.category)
    
    # Save results
    paths = save_results(results, output_dir)
    
    # Print summary
    print_summary(results, paths)


if __name__ == "__main__":
    asyncio.run(main())
