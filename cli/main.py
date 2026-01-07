"""Main CLI entry point for the review scraper."""

import asyncio
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table

from config.settings import settings
from config.logging_config import setup_logging

# Initialize
app = typer.Typer(
    name="review-scraper",
    help="Web scraper for collecting customer reviews from multiple sources.",
    add_completion=False,
)
console = Console()


@app.callback()
def callback():
    """Review Scraper - Collect customer reviews for AI training data."""
    pass


@app.command()
def scrape(
    source: str = typer.Argument(..., help="Source to scrape (e.g., 'trustpilot', 'imdb', 'steam')"),
    url: Optional[str] = typer.Option(None, "--url", "-u", help="Specific URL to scrape"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output file path"),
    max_reviews: Optional[int] = typer.Option(None, "--max", "-m", help="Maximum reviews to collect"),
    max_pages: Optional[int] = typer.Option(None, "--pages", "-p", help="Maximum pages to scrape"),
    include_metadata: bool = typer.Option(False, "--metadata", help="Include full metadata in output"),
):
    """
    Scrape reviews from a source.
    
    Examples:
        review-scraper scrape trustpilot --url https://www.trustpilot.com/review/amazon.com
        review-scraper scrape steam --url 1245620 --max 1000
        review-scraper scrape imdb --url tt0111161
    """
    setup_logging()
    
    # Ensure directories exist
    settings.ensure_directories()
    
    # Run async scraper
    asyncio.run(_scrape_async(
        source=source,
        url=url,
        output=output,
        max_reviews=max_reviews,
        max_pages=max_pages,
        include_metadata=include_metadata,
    ))


async def _scrape_async(
    source: str,
    url: Optional[str],
    output: Optional[str],
    max_reviews: Optional[int],
    max_pages: Optional[int],
    include_metadata: bool,
):
    """Async scraping implementation."""
    from src.scrapers import get_scraper
    from src.storage.json_storage import JsonStorage, export_to_training_format
    from src.models.source import load_sources
    
    console.print(f"\n[bold blue]Review Scraper[/bold blue] - {source}")
    console.print("=" * 50)
    
    # Get scraper class
    scraper_cls = get_scraper(f"review_platforms.{source}")
    if not scraper_cls:
        scraper_cls = get_scraper(f"entertainment.{source}")
    if not scraper_cls:
        scraper_cls = get_scraper(f"apps.{source}")
    
    if not scraper_cls:
        console.print(f"[red]Error:[/red] Unknown source '{source}'")
        console.print("\nAvailable sources: trustpilot, sitejabber, imdb, steam")
        raise typer.Exit(1)
    
    # Determine URLs to scrape
    urls = []
    if url:
        urls = [url]
    else:
        # Load from config
        try:
            source_config = load_sources()
            src = source_config.get_source(source)
            if src:
                urls = src.all_urls
        except Exception as e:
            console.print(f"[yellow]Warning:[/yellow] Could not load source config: {e}")
    
    if not urls:
        console.print(f"[red]Error:[/red] No URLs specified. Use --url or configure in sources.yaml")
        raise typer.Exit(1)
    
    console.print(f"Source: [cyan]{source}[/cyan]")
    console.print(f"URLs: [cyan]{len(urls)}[/cyan]")
    if max_reviews:
        console.print(f"Max reviews: [cyan]{max_reviews}[/cyan]")
    console.print()
    
    # Collect reviews
    all_reviews = []
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(f"Scraping {source}...", total=len(urls))
        
        async with scraper_cls() as scraper:
            for scrape_url in urls:
                progress.update(task, description=f"Scraping: {scrape_url[:50]}...")
                
                try:
                    async for review in scraper.scrape_all_pages(
                        scrape_url,
                        max_pages=max_pages,
                        max_reviews=max_reviews,
                    ):
                        all_reviews.append(review)
                        
                        if max_reviews and len(all_reviews) >= max_reviews:
                            break
                    
                except Exception as e:
                    console.print(f"[yellow]Warning:[/yellow] Error scraping {scrape_url}: {e}")
                
                progress.advance(task)
                
                if max_reviews and len(all_reviews) >= max_reviews:
                    break
    
    console.print()
    console.print(f"[green]✓[/green] Collected [bold]{len(all_reviews)}[/bold] reviews")
    
    # Save output
    if all_reviews:
        output_path = output or str(settings.output_dir / f"{source}_reviews.json")
        
        if include_metadata:
            storage = JsonStorage(output_path, include_metadata=True)
            await storage.save(all_reviews)
        else:
            # Export in training data format
            export_to_training_format(all_reviews, output_path)
        
        console.print(f"[green]✓[/green] Saved to [cyan]{output_path}[/cyan]")
    else:
        console.print("[yellow]No reviews collected[/yellow]")


@app.command()
def sources():
    """List available sources and their status."""
    from src.models.source import load_sources
    
    console.print("\n[bold blue]Available Sources[/bold blue]")
    console.print("=" * 50)
    
    try:
        config = load_sources()
        
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Source")
        table.add_column("Enabled")
        table.add_column("URLs")
        table.add_column("Browser")
        table.add_column("Rate Limit")
        
        for name, source in config.sources.items():
            table.add_row(
                name,
                "✓" if source.enabled else "✗",
                str(len(source.all_urls)),
                "Yes" if source.requires_browser else "No",
                f"{source.rate_limit_rpm}/min",
            )
        
        console.print(table)
        
    except Exception as e:
        console.print(f"[red]Error loading sources:[/red] {e}")


@app.command()
def export(
    input_file: str = typer.Argument(..., help="Input JSON file with reviews"),
    output_file: str = typer.Argument(..., help="Output file path"),
    format: str = typer.Option("training", help="Output format: training, full, jsonl"),
    start_id: int = typer.Option(1, help="Starting ID for renumbering"),
):
    """Export reviews to different formats."""
    import json
    from src.models.review import Review
    from src.storage.json_storage import export_to_training_format
    
    console.print(f"\n[bold blue]Exporting Reviews[/bold blue]")
    
    # Load input
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    reviews = [Review(**item) for item in data]
    console.print(f"Loaded [cyan]{len(reviews)}[/cyan] reviews")
    
    if format == "training":
        count = export_to_training_format(reviews, output_file, start_id=start_id)
        console.print(f"[green]✓[/green] Exported {count} reviews in training format")
    
    elif format == "full":
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump([r.to_full_dict() for r in reviews], f, ensure_ascii=False, indent=2)
        console.print(f"[green]✓[/green] Exported with full metadata")
    
    elif format == "jsonl":
        with open(output_file, "w", encoding="utf-8") as f:
            for review in reviews:
                f.write(json.dumps(review.to_export_dict(), ensure_ascii=False) + "\n")
        console.print(f"[green]✓[/green] Exported as JSON Lines")


@app.command()
def info():
    """Show configuration and system information."""
    console.print("\n[bold blue]Review Scraper Configuration[/bold blue]")
    console.print("=" * 50)
    
    table = Table(show_header=False)
    table.add_column("Setting", style="cyan")
    table.add_column("Value")
    
    table.add_row("Environment", settings.environment)
    table.add_row("Debug", str(settings.debug))
    table.add_row("Output Dir", str(settings.output_dir))
    table.add_row("Max Concurrent", str(settings.max_concurrent_requests))
    table.add_row("Request Delay", f"{settings.min_request_delay}-{settings.max_request_delay}s")
    table.add_row("Rate Limit", f"{settings.rate_limit_rpm}/min")
    table.add_row("Timeout", f"{settings.request_timeout}s")
    table.add_row("Max Retries", str(settings.max_retries))
    
    console.print(table)


def main():
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()