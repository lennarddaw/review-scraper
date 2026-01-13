"""
ADAC Review Scraper - FREE EDITION (No API Keys!)

Scrapes ADAC reviews from multiple FREE German platforms:
- Yelp.de
- GoLocal.de  
- GelbeSeiten.de
- 11880.com
- KennstDuEinen.de
- WerKenntDenBesten.de

All platforms use simple HTML scraping - no browser automation,
no API keys, completely FREE and UNLIMITED!

Usage:
    python scripts/scrape_adac_free.py
    python scripts/scrape_adac_free.py --max-reviews 50
    python scripts/scrape_adac_free.py --cities München Berlin Hamburg
"""

import argparse
import asyncio
import json
import signal
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from loguru import logger

console = Console()

# ============================================================================
# GERMAN CITIES TO SEARCH
# ============================================================================

GERMAN_CITIES = [
    # Top 50 German cities by population
    "München", "Berlin", "Hamburg", "Köln", "Frankfurt", "Stuttgart",
    "Düsseldorf", "Dortmund", "Essen", "Leipzig", "Bremen", "Dresden",
    "Hannover", "Nürnberg", "Duisburg", "Bochum", "Wuppertal", "Bielefeld",
    "Bonn", "Münster", "Karlsruhe", "Mannheim", "Augsburg", "Wiesbaden",
    "Mönchengladbach", "Gelsenkirchen", "Braunschweig", "Aachen", "Kiel",
    "Chemnitz", "Halle", "Magdeburg", "Freiburg", "Krefeld", "Lübeck",
    "Mainz", "Erfurt", "Oberhausen", "Rostock", "Kassel", "Hagen",
    "Potsdam", "Saarbrücken", "Hamm", "Ludwigshafen", "Oldenburg",
    "Osnabrück", "Leverkusen", "Heidelberg", "Darmstadt",
    # Additional important cities
    "Regensburg", "Würzburg", "Ingolstadt", "Ulm", "Heilbronn",
    "Pforzheim", "Göttingen", "Wolfsburg", "Reutlingen", "Koblenz",
    "Trier", "Jena", "Gera", "Cottbus", "Siegen", "Hildesheim",
]

# ============================================================================
# SCRAPER CONFIGURATION - ALL FREE, NO API KEYS!
# ============================================================================

SCRAPERS = {
    "yelp_de": {
        "module": "src.scrapers.local.yelp_de",
        "class": "YelpDeScraper",
        "search_template": "ADAC {city}",
        "description": "Yelp Deutschland",
    },
    "golocal": {
        "module": "src.scrapers.local.golocal", 
        "class": "GoLocalScraper",
        "search_template": "ADAC {city}",
        "description": "GoLocal.de",
    },
    "gelbe_seiten": {
        "module": "src.scrapers.local.gelbe_seiten",
        "class": "GelbeSeitenScraper", 
        "search_template": "ADAC {city}",
        "description": "Gelbe Seiten",
    },
    "11880": {
        "module": "src.scrapers.local.scraper_11880",
        "class": "Scraper11880",
        "search_template": "ADAC {city}",
        "description": "11880.com",
    },
    "kennstdueinen": {
        "module": "src.scrapers.local.kennstdueinen",
        "class": "KennstDuEinenScraper",
        "search_template": "ADAC {city}",
        "description": "KennstDuEinen.de",
    },
    "werkenntdenbesten": {
        "module": "src.scrapers.local.werkenntdenbesten",
        "class": "WerKenntDenBestenScraper",
        "search_template": "ADAC {city}",
        "description": "WerKenntDenBesten.de",
    },
}


# ============================================================================
# GLOBAL STATE
# ============================================================================

all_reviews = []
interrupted = False
stats = {}


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully."""
    global interrupted
    if interrupted:
        console.print("\n[red]Force quit! Saving collected data...[/red]")
        save_results()
        sys.exit(1)
    else:
        interrupted = True
        console.print("\n[yellow]⚠ Interrupt received! Finishing current source and saving...[/yellow]")
        console.print("[yellow]  Press Ctrl+C again to force quit[/yellow]")


def load_scraper(name: str):
    """Dynamically load a scraper class."""
    config = SCRAPERS[name]
    module = __import__(config["module"], fromlist=[config["class"]])
    return getattr(module, config["class"])


def scrape_source(scraper_name: str, cities: list[str], max_per_city: int) -> list:
    """Scrape a single source for all cities."""
    global interrupted
    
    config = SCRAPERS[scraper_name]
    reviews = []
    
    try:
        ScraperClass = load_scraper(scraper_name)
        scraper = ScraperClass()
        
        for city in cities:
            if interrupted:
                break
            
            search_query = config["search_template"].format(city=city)
            
            try:
                city_reviews = scraper.scrape_reviews(search_query, max_reviews=max_per_city)
                reviews.extend(city_reviews)
                
                if city_reviews:
                    console.print(f"    [green]✓[/green] {city}: {len(city_reviews)} reviews")
                    
            except Exception as e:
                logger.debug(f"[{scraper_name}] Error for {city}: {e}")
                continue
        
    except Exception as e:
        logger.error(f"[{scraper_name}] Failed to load scraper: {e}")
    
    return reviews


def save_results():
    """Save all collected reviews."""
    global all_reviews, stats
    
    if not all_reviews:
        console.print("[yellow]No reviews to save.[/yellow]")
        return
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create exports directory
    exports_dir = Path("data/exports")
    exports_dir.mkdir(parents=True, exist_ok=True)
    
    # Training format (just text)
    training_file = exports_dir / f"adac_free_training_{timestamp}.json"
    training_data = [{"id": i+1, "text": r.text} for i, r in enumerate(all_reviews)]
    with open(training_file, 'w', encoding='utf-8') as f:
        json.dump(training_data, f, ensure_ascii=False, indent=2)
    
    # Full data
    full_file = exports_dir / f"adac_free_all_{timestamp}.json"
    full_data = [r.to_dict() if hasattr(r, 'to_dict') else {
        "text": r.text,
        "source": r.source,
        "rating": r.rating,
        "author": r.author,
        "date": r.date.isoformat() if r.date else None,
    } for r in all_reviews]
    with open(full_file, 'w', encoding='utf-8') as f:
        json.dump(full_data, f, ensure_ascii=False, indent=2)
    
    console.print(f"\n[green]✓[/green] Training file: {training_file.absolute()}")
    console.print(f"[green]✓[/green] Full data file: {full_file.absolute()}")


def print_summary():
    """Print scraping summary."""
    global stats
    
    console.print("\n" + "═" * 60)
    console.print("[bold]SCRAPING SUMMARY - ADAC FREE EDITION[/bold]")
    console.print("═" * 60)
    
    table = Table(show_header=True, header_style="bold")
    table.add_column("Source")
    table.add_column("Reviews", justify="right")
    
    total = 0
    for source, count in sorted(stats.items()):
        table.add_row(source, str(count))
        total += count
    
    table.add_row("─" * 20, "─" * 8)
    table.add_row("[bold]TOTAL[/bold]", f"[bold]{total}[/bold]")
    
    console.print(table)


def main():
    global all_reviews, stats, interrupted
    
    parser = argparse.ArgumentParser(description="ADAC Review Scraper - FREE Edition")
    parser.add_argument("--max-reviews", type=int, default=30,
                       help="Max reviews per city per source (default: 30)")
    parser.add_argument("--cities", nargs="+", default=None,
                       help="Specific cities to scrape (default: all)")
    parser.add_argument("--sources", nargs="+", default=None,
                       choices=list(SCRAPERS.keys()),
                       help="Specific sources to use")
    
    args = parser.parse_args()
    
    # Setup signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    cities = args.cities or GERMAN_CITIES
    sources = args.sources or list(SCRAPERS.keys())
    
    console.print("[bold blue]═══════════════════════════════════════════════════════════[/bold blue]")
    console.print("[bold blue]   ADAC REVIEW SCRAPER - FREE EDITION (No API Keys!)[/bold blue]")
    console.print("[bold blue]═══════════════════════════════════════════════════════════[/bold blue]")
    console.print(f"\nSources: {len(sources)}")
    console.print(f"Cities: {len(cities)}")
    console.print(f"Max reviews per city: {args.max_reviews}")
    console.print(f"Potential max: ~{len(sources) * len(cities) * args.max_reviews:,} reviews")
    console.print("\n[dim]Press Ctrl+C to stop and save current progress[/dim]\n")
    
    for source_name in sources:
        if interrupted:
            break
        
        config = SCRAPERS[source_name]
        console.print(f"\n[bold cyan]► {config['description'].upper()}[/bold cyan]")
        
        reviews = scrape_source(source_name, cities, args.max_reviews)
        
        stats[source_name] = len(reviews)
        all_reviews.extend(reviews)
        
        console.print(f"  [bold]Subtotal: {len(reviews)} reviews[/bold]")
    
    # Print summary and save
    print_summary()
    save_results()
    
    console.print(f"\n[bold green]═══ GRAND TOTAL: {len(all_reviews)} REVIEWS ═══[/bold green]")


if __name__ == "__main__":
    main()