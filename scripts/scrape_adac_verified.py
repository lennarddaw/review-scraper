"""
ADAC Review Scraper - VERIFIED SOURCES ONLY

Scrapes from platforms with CONFIRMED reviews:
1. Trustpilot.de - 6,368+ reviews
2. Finanzfluss.de - 154+ reviews

Usage:
    python scripts/scrape_adac_verified.py
    python scripts/scrape_adac_verified.py --max-reviews 100
    python scripts/scrape_adac_verified.py --source trustpilot
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


class TrustpilotScraper:
    """Trustpilot ADAC Scraper - 6,368+ reviews available."""
    
    name = "trustpilot"
    base_url = "https://de.trustpilot.com/review/www.adac.de"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9',
        })
    
    def scrape(self, max_reviews=500, max_pages=25):
        """Scrape reviews from Trustpilot."""
        reviews = []
        
        for page in range(1, max_pages + 1):
            if len(reviews) >= max_reviews:
                break
                
            url = f"{self.base_url}?page={page}" if page > 1 else self.base_url
            print(f"  [Trustpilot] Page {page}...", end=" ", flush=True)
            
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                
                page_reviews = self._parse_page(resp.text, url)
                reviews.extend(page_reviews)
                print(f"{len(page_reviews)} reviews")
                
                if not page_reviews:
                    print("  No more reviews found.")
                    break
                
                time.sleep(1.5)
                
            except Exception as e:
                print(f"Error: {e}")
                break
        
        return reviews[:max_reviews]
    
    def _parse_page(self, html, source_url):
        """Parse reviews from HTML."""
        soup = BeautifulSoup(html, 'html.parser')
        reviews = []
        
        # Find all review sections by looking for star rating images
        # Trustpilot uses img tags with src containing "stars-X"
        star_images = soup.find_all('img', {'src': re.compile(r'stars-\d')})
        
        seen_texts = set()
        
        for star_img in star_images:
            try:
                # Get rating from image src
                src = star_img.get('src', '')
                rating_match = re.search(r'stars-(\d)', src)
                rating = float(rating_match.group(1)) if rating_match else None
                
                # Find the review container
                # Go up the tree to find a container with substantial text
                container = star_img
                for _ in range(10):
                    parent = container.parent
                    if parent is None:
                        break
                    text = parent.get_text(strip=True)
                    # Good container has review-like length
                    if 100 < len(text) < 3000:
                        container = parent
                        break
                    container = parent
                
                # Extract text
                text = container.get_text(separator=' ', strip=True)
                
                # Skip if too short or already seen
                if len(text) < 50:
                    continue
                
                # Clean up text - remove common UI elements
                text = self._clean_text(text)
                
                if len(text) < 30:
                    continue
                
                # Check for duplicate
                text_hash = text[:100]
                if text_hash in seen_texts:
                    continue
                seen_texts.add(text_hash)
                
                # Extract date
                date = self._extract_date(container.get_text())
                
                # Extract author
                author = None
                user_link = container.find('a', href=re.compile(r'/users/'))
                if user_link:
                    author = user_link.get_text(strip=True)
                
                reviews.append({
                    'text': f"[ADAC Trustpilot] {text[:800]}",
                    'rating': rating,
                    'author': author,
                    'date': date,
                    'source': 'trustpilot.de',
                    'source_url': source_url,
                })
                
            except Exception as e:
                continue
        
        return reviews
    
    def _clean_text(self, text):
        """Clean review text."""
        # Remove common UI elements
        removes = [
            r'Bewertet mit \d+ von 5 Sternen?',
            r'Unternehmen hat geantwortet',
            r'Mehr ansehen',
            r'Link kopieren',
            r'Melden',
            r'Nächste Seite',
            r'Zurück',
            r'\d+ Bewertungen?',
            r'Bewertung abgeben',
            r'Zur Website',
        ]
        
        for pattern in removes:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)
        
        # Remove multiple spaces
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def _extract_date(self, text):
        """Extract date from text."""
        # "Vor X Tagen"
        match = re.search(r'Vor\s+(\d+)\s+Tag', text)
        if match:
            days = int(match.group(1))
            return (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        # "Vor X Stunden"
        match = re.search(r'Vor\s+(\d+)\s+Stund', text)
        if match:
            hours = int(match.group(1))
            return (datetime.now() - timedelta(hours=hours)).strftime('%Y-%m-%d')
        
        # "X. Jan. 2026"
        months = {'jan': 1, 'feb': 2, 'mär': 3, 'mar': 3, 'apr': 4, 'mai': 5, 'jun': 6, 
                  'jul': 7, 'aug': 8, 'sep': 9, 'okt': 10, 'nov': 11, 'dez': 12}
        match = re.search(r'(\d{1,2})\.\s*(Jan|Feb|Mär|Mar|Apr|Mai|Jun|Jul|Aug|Sep|Okt|Nov|Dez)\w*\.?\s*(\d{4})', text, re.I)
        if match:
            day = int(match.group(1))
            month = months.get(match.group(2).lower()[:3], 1)
            year = int(match.group(3))
            return f"{year}-{month:02d}-{day:02d}"
        
        return None


class FinanzflussScraper:
    """Finanzfluss ADAC Scraper - 154+ reviews available."""
    
    name = "finanzfluss"
    base_url = "https://www.finanzfluss.de/anbieter/adac/erfahrungen/"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'de-DE,de;q=0.9',
        })
    
    def scrape(self, max_reviews=200):
        """Scrape reviews from Finanzfluss."""
        print(f"  [Finanzfluss] Fetching...", end=" ", flush=True)
        
        try:
            resp = self.session.get(self.base_url, timeout=30)
            resp.raise_for_status()
            
            reviews = self._parse_page(resp.text)
            print(f"{len(reviews)} reviews")
            
            return reviews[:max_reviews]
            
        except Exception as e:
            print(f"Error: {e}")
            return []
    
    def _parse_page(self, html):
        """Parse reviews from HTML."""
        soup = BeautifulSoup(html, 'html.parser')
        reviews = []
        
        # Find review blocks by the "ADAC Erfahrung #" pattern
        text = soup.get_text()
        
        # Split by experience markers
        pattern = r'(ADAC Erfahrung #\d+)'
        parts = re.split(pattern, text)
        
        for i in range(1, len(parts), 2):
            if i + 1 >= len(parts):
                break
                
            header = parts[i]
            content = parts[i + 1]
            
            # Extract review number
            num_match = re.search(r'#(\d+)', header)
            num = num_match.group(1) if num_match else "?"
            
            # Extract rating
            rating = None
            rating_match = re.search(r'(\d)[,.](\d)\s*von\s*5|(\d)\s*von\s*5', content)
            if rating_match:
                if rating_match.group(3):
                    rating = float(rating_match.group(3))
                elif rating_match.group(1) and rating_match.group(2):
                    rating = float(f"{rating_match.group(1)}.{rating_match.group(2)}")
            
            # Extract date
            date = None
            date_match = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', content)
            if date_match:
                date = f"{date_match.group(3)}-{date_match.group(2)}-{date_match.group(1)}"
            
            # Extract author
            author = None
            author_match = re.search(r'Bewertung von\s+([A-Za-zäöüÄÖÜß\s]+?)\s+am', content)
            if author_match:
                author = author_match.group(1).strip()
            
            # Extract review text
            # Remove metadata
            review_text = content
            review_text = re.sub(r'\d[,.]?\d?\s*von\s*5\s*Stern\w*', '', review_text)
            review_text = re.sub(r'Bewertung von.*?am\s*\d{2}\.\d{2}\.\d{4}', '', review_text)
            review_text = re.sub(r'Link kopieren|Melden', '', review_text)
            review_text = re.sub(r'\s+', ' ', review_text).strip()
            
            # Get first substantial chunk
            chunks = [c.strip() for c in review_text.split('.') if len(c.strip()) > 20]
            if chunks:
                # Rejoin sentences
                review_text = '. '.join(chunks[:5])[:800]
            
            if len(review_text) < 30:
                continue
            
            reviews.append({
                'text': f"[ADAC Finanzfluss #{num}] {review_text}",
                'rating': rating,
                'author': author,
                'date': date,
                'source': 'finanzfluss.de',
                'source_url': self.base_url,
            })
        
        return reviews


def main():
    parser = argparse.ArgumentParser(description="ADAC Review Scraper - Verified Sources")
    parser.add_argument('--max-reviews', type=int, default=200, help='Max reviews to collect')
    parser.add_argument('--source', choices=['trustpilot', 'finanzfluss', 'all'], default='all', help='Source to scrape')
    parser.add_argument('--output', type=str, default=None, help='Output file path')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print(" ADAC REVIEW SCRAPER - VERIFIED SOURCES")
    print("=" * 60)
    print(f"\nSources: Trustpilot (6,368+) + Finanzfluss (154+)")
    print(f"Max reviews: {args.max_reviews}")
    print()
    
    all_reviews = []
    
    # Scrape Trustpilot
    if args.source in ['trustpilot', 'all']:
        print("📊 TRUSTPILOT.DE")
        scraper = TrustpilotScraper()
        reviews = scraper.scrape(max_reviews=args.max_reviews)
        all_reviews.extend(reviews)
        print(f"   Total: {len(reviews)} reviews\n")
    
    # Scrape Finanzfluss
    if args.source in ['finanzfluss', 'all']:
        print("📊 FINANZFLUSS.DE")
        scraper = FinanzflussScraper()
        reviews = scraper.scrape(max_reviews=args.max_reviews)
        all_reviews.extend(reviews)
        print(f"   Total: {len(reviews)} reviews\n")
    
    # Save results
    if all_reviews:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Create output directory
        output_dir = Path('data/exports')
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Output file
        if args.output:
            output_file = Path(args.output)
        else:
            output_file = output_dir / f'adac_verified_{timestamp}.json'
        
        # Save full data
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_reviews, f, ensure_ascii=False, indent=2)
        
        # Save training format (text only)
        training_file = output_dir / f'adac_training_{timestamp}.json'
        training_data = [{'id': i+1, 'text': r['text']} for i, r in enumerate(all_reviews)]
        with open(training_file, 'w', encoding='utf-8') as f:
            json.dump(training_data, f, ensure_ascii=False, indent=2)
        
        print("=" * 60)
        print(f"✅ TOTAL: {len(all_reviews)} reviews collected")
        print(f"📁 Full data: {output_file}")
        print(f"📁 Training: {training_file}")
        print("=" * 60)
    else:
        print("❌ No reviews collected!")


if __name__ == "__main__":
    main()
