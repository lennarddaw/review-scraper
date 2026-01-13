#!/usr/bin/env python3
"""
ADAC Review Scraper - STANDALONE VERSION
=========================================

VERIFIED SOURCES (as of January 2026):
- Trustpilot.de: 6,368+ ADAC reviews  
- Finanzfluss.de: 154+ ADAC reviews

Requirements:
    pip install requests beautifulsoup4

Usage:
    python adac_scraper_standalone.py
    python adac_scraper_standalone.py --max-pages 10
"""

import json
import re
import sys
import time
from datetime import datetime, timedelta

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: Missing dependencies!")
    print("Run: pip install requests beautifulsoup4")
    sys.exit(1)


# =============================================================================
# TRUSTPILOT SCRAPER - 6,368+ Reviews
# =============================================================================

def scrape_trustpilot(max_pages=10):
    """
    Scrape ADAC reviews from Trustpilot.de
    
    URL: https://de.trustpilot.com/review/www.adac.de
    """
    base_url = "https://de.trustpilot.com/review/www.adac.de"
    reviews = []
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'de-DE,de;q=0.9',
    })
    
    print(f"\n{'='*60}")
    print("TRUSTPILOT.DE - ADAC Reviews")
    print(f"{'='*60}")
    
    for page in range(1, max_pages + 1):
        url = f"{base_url}?page={page}" if page > 1 else base_url
        print(f"Page {page}...", end=" ", flush=True)
        
        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            page_reviews = []
            
            # Find reviews by star rating images (most reliable indicator)
            star_imgs = soup.find_all('img', src=lambda x: x and 'stars-' in str(x))
            
            seen = set()
            for star_img in star_imgs:
                try:
                    # Extract rating
                    src = star_img.get('src', '')
                    rating_match = re.search(r'stars-(\d)', src)
                    rating = float(rating_match.group(1)) if rating_match else None
                    
                    # Find container with review text
                    container = star_img
                    for _ in range(8):
                        parent = container.parent
                        if parent is None:
                            break
                        text = parent.get_text(strip=True)
                        if 100 < len(text) < 2500:
                            container = parent
                            break
                        container = parent
                    
                    # Get text
                    text = container.get_text(separator=' ', strip=True)
                    
                    # Clean up
                    for remove in ['Bewertet mit', 'von 5 Sternen', 'Unternehmen hat geantwortet', 
                                   'Mehr ansehen', 'Link kopieren', 'Melden']:
                        text = text.replace(remove, '')
                    text = re.sub(r'\s+', ' ', text).strip()
                    
                    if len(text) < 40:
                        continue
                    
                    # Avoid duplicates
                    text_key = text[:80]
                    if text_key in seen:
                        continue
                    seen.add(text_key)
                    
                    # Extract author
                    author = None
                    user_link = container.find('a', href=lambda x: x and '/users/' in str(x))
                    if user_link:
                        author = user_link.get_text(strip=True)
                    
                    page_reviews.append({
                        'text': f"[ADAC Trustpilot] {text[:700]}",
                        'rating': rating,
                        'author': author,
                        'source': 'trustpilot.de',
                    })
                    
                except Exception:
                    continue
            
            reviews.extend(page_reviews)
            print(f"{len(page_reviews)} reviews (total: {len(reviews)})")
            
            if len(page_reviews) == 0:
                print("No more reviews found, stopping.")
                break
            
            time.sleep(1.5)
            
        except Exception as e:
            print(f"Error: {e}")
            break
    
    return reviews


# =============================================================================
# FINANZFLUSS SCRAPER - 154+ Reviews
# =============================================================================

def scrape_finanzfluss():
    """
    Scrape ADAC reviews from Finanzfluss.de
    
    URL: https://www.finanzfluss.de/anbieter/adac/erfahrungen/
    """
    url = "https://www.finanzfluss.de/anbieter/adac/erfahrungen/"
    reviews = []
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'de-DE,de;q=0.9',
    })
    
    print(f"\n{'='*60}")
    print("FINANZFLUSS.DE - ADAC Reviews")
    print(f"{'='*60}")
    
    print("Fetching page...", end=" ", flush=True)
    
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        
        text = resp.text
        
        # Find reviews by "ADAC Erfahrung #" pattern
        pattern = r'ADAC Erfahrung #(\d+)'
        parts = re.split(f'({pattern})', text)
        
        for i in range(1, len(parts), 2):
            if 'ADAC Erfahrung #' not in parts[i]:
                continue
            
            if i + 1 >= len(parts):
                break
            
            num_match = re.search(r'#(\d+)', parts[i])
            num = num_match.group(1) if num_match else "?"
            
            content = parts[i + 1][:2000]  # Limit content size
            
            # Extract rating
            rating = None
            rating_match = re.search(r'(\d)[,.](\d)\s*von\s*5|(\d)\s*von\s*5', content)
            if rating_match:
                if rating_match.group(3):
                    rating = float(rating_match.group(3))
                else:
                    rating = float(f"{rating_match.group(1)}.{rating_match.group(2)}")
            
            # Extract date
            date = None
            date_match = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', content)
            if date_match:
                date = f"{date_match.group(3)}-{date_match.group(2)}-{date_match.group(1)}"
            
            # Clean review text
            review_text = content
            review_text = re.sub(r'\d[,.]?\d?\s*von\s*5\s*Stern\w*', '', review_text)
            review_text = re.sub(r'Bewertung von.*?am\s*\d{2}\.\d{2}\.\d{4}', '', review_text)
            review_text = re.sub(r'Link kopieren|Melden', '', review_text)
            review_text = re.sub(r'\s+', ' ', review_text).strip()
            
            # Get first substantial paragraph
            sentences = [s.strip() for s in review_text.split('.') if len(s.strip()) > 15]
            if sentences:
                review_text = '. '.join(sentences[:6])[:700]
            
            if len(review_text) < 30:
                continue
            
            reviews.append({
                'text': f"[ADAC Finanzfluss #{num}] {review_text}",
                'rating': rating,
                'date': date,
                'source': 'finanzfluss.de',
            })
        
        print(f"{len(reviews)} reviews")
        
    except Exception as e:
        print(f"Error: {e}")
    
    return reviews


# =============================================================================
# MAIN
# =============================================================================

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='ADAC Review Scraper - Standalone')
    parser.add_argument('--max-pages', type=int, default=10, help='Max Trustpilot pages (default: 10)')
    parser.add_argument('--output', type=str, default='adac_reviews.json', help='Output file')
    parser.add_argument('--source', choices=['all', 'trustpilot', 'finanzfluss'], default='all')
    args = parser.parse_args()
    
    print("\n" + "="*60)
    print(" ADAC REVIEW SCRAPER - STANDALONE")
    print(" Verified Sources: Trustpilot (6,368+) + Finanzfluss (154+)")
    print("="*60)
    
    all_reviews = []
    
    if args.source in ['all', 'trustpilot']:
        reviews = scrape_trustpilot(max_pages=args.max_pages)
        all_reviews.extend(reviews)
    
    if args.source in ['all', 'finanzfluss']:
        reviews = scrape_finanzfluss()
        all_reviews.extend(reviews)
    
    # Save results
    if all_reviews:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(all_reviews, f, ensure_ascii=False, indent=2)
        
        # Also save training format
        training_file = args.output.replace('.json', '_training.json')
        training_data = [{'id': i+1, 'text': r['text']} for i, r in enumerate(all_reviews)]
        with open(training_file, 'w', encoding='utf-8') as f:
            json.dump(training_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n{'='*60}")
        print(f" TOTAL: {len(all_reviews)} reviews collected!")
        print(f" Full data: {args.output}")
        print(f" Training:  {training_file}")
        print(f"{'='*60}")
        
        # Show sample
        print("\nSample reviews:")
        for i, r in enumerate(all_reviews[:3]):
            print(f"\n[{i+1}] Rating: {r.get('rating')} | {r['text'][:100]}...")
    else:
        print("\nNo reviews collected!")


if __name__ == '__main__':
    main()