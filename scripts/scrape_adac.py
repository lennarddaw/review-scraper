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
            # =====================================================
            # ADAC GESCHÄFTSSTELLEN - Alle Bundesländer & Großstädte
            # =====================================================
            
            # Bayern
            "ADAC Geschäftsstelle München",
            "ADAC Geschäftsstelle Nürnberg",
            "ADAC Geschäftsstelle Augsburg",
            "ADAC Geschäftsstelle Regensburg",
            "ADAC Geschäftsstelle Würzburg",
            "ADAC Geschäftsstelle Ingolstadt",
            "ADAC Geschäftsstelle Fürth",
            "ADAC Geschäftsstelle Erlangen",
            "ADAC Geschäftsstelle Bayreuth",
            "ADAC Geschäftsstelle Bamberg",
            "ADAC Geschäftsstelle Passau",
            "ADAC Geschäftsstelle Rosenheim",
            "ADAC Geschäftsstelle Landshut",
            
            # Baden-Württemberg
            "ADAC Geschäftsstelle Stuttgart",
            "ADAC Geschäftsstelle Mannheim",
            "ADAC Geschäftsstelle Karlsruhe",
            "ADAC Geschäftsstelle Freiburg",
            "ADAC Geschäftsstelle Heidelberg",
            "ADAC Geschäftsstelle Ulm",
            "ADAC Geschäftsstelle Heilbronn",
            "ADAC Geschäftsstelle Pforzheim",
            "ADAC Geschäftsstelle Reutlingen",
            "ADAC Geschäftsstelle Tübingen",
            "ADAC Geschäftsstelle Konstanz",
            "ADAC Geschäftsstelle Ludwigsburg",
            "ADAC Geschäftsstelle Esslingen",
            
            # Nordrhein-Westfalen
            "ADAC Geschäftsstelle Köln",
            "ADAC Geschäftsstelle Düsseldorf",
            "ADAC Geschäftsstelle Dortmund",
            "ADAC Geschäftsstelle Essen",
            "ADAC Geschäftsstelle Duisburg",
            "ADAC Geschäftsstelle Bochum",
            "ADAC Geschäftsstelle Wuppertal",
            "ADAC Geschäftsstelle Bielefeld",
            "ADAC Geschäftsstelle Bonn",
            "ADAC Geschäftsstelle Münster",
            "ADAC Geschäftsstelle Mönchengladbach",
            "ADAC Geschäftsstelle Gelsenkirchen",
            "ADAC Geschäftsstelle Aachen",
            "ADAC Geschäftsstelle Krefeld",
            "ADAC Geschäftsstelle Oberhausen",
            "ADAC Geschäftsstelle Hagen",
            "ADAC Geschäftsstelle Hamm",
            "ADAC Geschäftsstelle Leverkusen",
            "ADAC Geschäftsstelle Solingen",
            "ADAC Geschäftsstelle Neuss",
            "ADAC Geschäftsstelle Paderborn",
            "ADAC Geschäftsstelle Siegen",
            "ADAC Geschäftsstelle Recklinghausen",
            
            # Hessen
            "ADAC Geschäftsstelle Frankfurt",
            "ADAC Geschäftsstelle Wiesbaden",
            "ADAC Geschäftsstelle Kassel",
            "ADAC Geschäftsstelle Darmstadt",
            "ADAC Geschäftsstelle Offenbach",
            "ADAC Geschäftsstelle Gießen",
            "ADAC Geschäftsstelle Marburg",
            "ADAC Geschäftsstelle Fulda",
            
            # Niedersachsen
            "ADAC Geschäftsstelle Hannover",
            "ADAC Geschäftsstelle Braunschweig",
            "ADAC Geschäftsstelle Oldenburg",
            "ADAC Geschäftsstelle Osnabrück",
            "ADAC Geschäftsstelle Göttingen",
            "ADAC Geschäftsstelle Wolfsburg",
            "ADAC Geschäftsstelle Hildesheim",
            "ADAC Geschäftsstelle Salzgitter",
            "ADAC Geschäftsstelle Wilhelmshaven",
            "ADAC Geschäftsstelle Celle",
            "ADAC Geschäftsstelle Lüneburg",
            
            # Berlin & Brandenburg
            "ADAC Geschäftsstelle Berlin",
            "ADAC Geschäftsstelle Berlin Mitte",
            "ADAC Geschäftsstelle Berlin Charlottenburg",
            "ADAC Geschäftsstelle Berlin Spandau",
            "ADAC Geschäftsstelle Potsdam",
            "ADAC Geschäftsstelle Cottbus",
            "ADAC Geschäftsstelle Frankfurt Oder",
            
            # Sachsen
            "ADAC Geschäftsstelle Dresden",
            "ADAC Geschäftsstelle Leipzig",
            "ADAC Geschäftsstelle Chemnitz",
            "ADAC Geschäftsstelle Zwickau",
            "ADAC Geschäftsstelle Plauen",
            
            # Hamburg & Schleswig-Holstein
            "ADAC Geschäftsstelle Hamburg",
            "ADAC Geschäftsstelle Kiel",
            "ADAC Geschäftsstelle Lübeck",
            "ADAC Geschäftsstelle Flensburg",
            "ADAC Geschäftsstelle Neumünster",
            
            # Bremen
            "ADAC Geschäftsstelle Bremen",
            "ADAC Geschäftsstelle Bremerhaven",
            
            # Rheinland-Pfalz
            "ADAC Geschäftsstelle Mainz",
            "ADAC Geschäftsstelle Koblenz",
            "ADAC Geschäftsstelle Trier",
            "ADAC Geschäftsstelle Ludwigshafen",
            "ADAC Geschäftsstelle Kaiserslautern",
            
            # Saarland
            "ADAC Geschäftsstelle Saarbrücken",
            
            # Sachsen-Anhalt
            "ADAC Geschäftsstelle Magdeburg",
            "ADAC Geschäftsstelle Halle Saale",
            "ADAC Geschäftsstelle Dessau",
            
            # Thüringen
            "ADAC Geschäftsstelle Erfurt",
            "ADAC Geschäftsstelle Jena",
            "ADAC Geschäftsstelle Gera",
            "ADAC Geschäftsstelle Weimar",
            
            # Mecklenburg-Vorpommern
            "ADAC Geschäftsstelle Rostock",
            "ADAC Geschäftsstelle Schwerin",
            "ADAC Geschäftsstelle Stralsund",
            "ADAC Geschäftsstelle Greifswald",
            
            # =====================================================
            # ADAC FAHRSICHERHEITSZENTREN
            # =====================================================
            "ADAC Fahrsicherheitszentrum Berlin Brandenburg",
            "ADAC Fahrsicherheitszentrum Linthe",
            "ADAC Fahrsicherheitszentrum Grevenbroich",
            "ADAC Fahrsicherheitszentrum Augsburg",
            "ADAC Fahrsicherheitszentrum Hannover Laatzen",
            "ADAC Fahrsicherheitszentrum Koblenz",
            "ADAC Fahrsicherheitszentrum Nohra",
            "ADAC Fahrsicherheitszentrum Schlüsselfeld",
            "ADAC Fahrsicherheitszentrum Hockenheim",
            "ADAC Fahrsicherheitszentrum Nürburgring",
            "ADAC Fahrsicherheitszentrum Sachsenring",
            
            # =====================================================
            # ADAC PRÜFZENTREN & TECHNIKZENTREN
            # =====================================================
            "ADAC Prüfzentrum",
            "ADAC Technikzentrum Landsberg",
            "ADAC Testzentrum",
            
            # =====================================================
            # ADAC REISEBÜROS (zusätzlich zu Geschäftsstellen)
            # =====================================================
            "ADAC Reisebüro München",
            "ADAC Reisebüro Berlin",
            "ADAC Reisebüro Hamburg",
            "ADAC Reisebüro Frankfurt",
            "ADAC Reisebüro Köln",
            "ADAC Reisebüro Stuttgart",
            
            # =====================================================
            # ADAC ZENTRALE
            # =====================================================
            "ADAC Zentrale München",
            "ADAC Hauptsitz München Hansastraße",
        ],
        "max_per_url": 500,  # Get ALL reviews per location
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
    import signal
    
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
    
    # Store results globally for graceful shutdown
    results = {}
    interrupted = False
    
    def handle_interrupt(signum, frame):
        nonlocal interrupted
        if interrupted:
            console.print("\n[red]Force quit! Saving what we have...[/red]")
            if results:
                paths = save_results(results, output_dir)
                print_summary(results, paths)
            sys.exit(1)
        
        interrupted = True
        console.print("\n[yellow]⚠ Interrupt received! Finishing current scrape and saving...[/yellow]")
        console.print("[yellow]  Press Ctrl+C again to force quit[/yellow]")
    
    # Register signal handler
    signal.signal(signal.SIGINT, handle_interrupt)
    
    try:
        # Run scraping with interrupt check
        results = await scrape_all_adac_interruptible(args.max_reviews, args.category, lambda: interrupted)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
    
    # Save results (even if interrupted)
    if results:
        paths = save_results(results, output_dir)
        print_summary(results, paths)
        
        if interrupted:
            console.print("\n[yellow]⚠ Scraping was interrupted but data was saved![/yellow]")
    else:
        console.print("[red]No data collected.[/red]")


async def scrape_all_adac_interruptible(
    max_reviews_per_source: int | None = None, 
    categories: list[str] | None = None,
    check_interrupted: callable = None
) -> dict[str, list[Review]]:
    """Scrape all ADAC sources with interrupt support."""
    results = {}
    
    console.print("\n[bold blue]═══════════════════════════════════════════════════[/bold blue]")
    console.print("[bold blue]  ADAC COMPREHENSIVE GERMAN FEEDBACK COLLECTOR[/bold blue]")
    console.print("[bold blue]═══════════════════════════════════════════════════[/bold blue]")
    console.print(f"\nTarget: [bold]15,000+[/bold] German feedbacks")
    console.print(f"Categories: {categories or 'ALL'}")
    console.print("[dim]Press Ctrl+C to stop and save current progress[/dim]\n")
    
    for source_name, config in ADAC_SOURCES.items():
        # Check if interrupted
        if check_interrupted and check_interrupted():
            console.print(f"\n[yellow]Stopping... Skipping remaining sources[/yellow]")
            break
        
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


if __name__ == "__main__":
    asyncio.run(main())