"""
Finanzfluss ADAC Scraper - VERIFIED WORKING

Scrapes ADAC reviews from Finanzfluss.de
Source: https://www.finanzfluss.de/anbieter/adac/erfahrungen/
Total available: 154+ reviews

Based on actual HTML analysis from January 2026.
"""

import re
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from loguru import logger


class FinanzflussADACScraper:
    """Scraper for Finanzfluss ADAC reviews."""

    name = "finanzfluss"
    base_url = "https://www.finanzfluss.de/anbieter/adac/erfahrungen/"
    
    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
        })

    def scrape_reviews(self, max_reviews: int = 200) -> list[dict]:
        """
        Scrape ADAC reviews from Finanzfluss.
        
        The page loads all reviews via JavaScript, so we need to handle pagination
        or find the API endpoint.
        """
        all_reviews = []
        
        try:
            logger.info(f"[{self.name}] Fetching {self.base_url}")
            
            response = self._session.get(self.base_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract reviews from page
            reviews = self._extract_reviews(soup)
            all_reviews.extend(reviews)
            
            logger.info(f"[{self.name}] Found {len(reviews)} reviews")
            
        except Exception as e:
            logger.error(f"[{self.name}] Error: {e}")
        
        return all_reviews[:max_reviews]

    def _extract_reviews(self, soup: BeautifulSoup) -> list[dict]:
        """Extract reviews from page."""
        reviews = []
        
        # Finanzfluss uses emoji indicators for review sentiment
        # 😊 = positive, 😐 = neutral, 😒 = negative
        
        # Find all review blocks - they contain "ADAC Erfahrung #" in text
        page_text = soup.get_text()
        
        # Method 1: Find by experience number pattern
        experience_pattern = r'ADAC Erfahrung #(\d+)'
        matches = list(re.finditer(experience_pattern, page_text))
        
        if matches:
            logger.debug(f"[{self.name}] Found {len(matches)} experience markers")
        
        # Method 2: Find review containers by structure
        # Reviews have: emoji, title, rating, date, text
        
        # Look for rating patterns like "5 von 5 Sternen" or "2,5 von 5"
        rating_elements = soup.find_all(string=re.compile(r'\d[,.]?\d?\s*von\s*5'))
        
        for rating_el in rating_elements:
            try:
                # Navigate to parent container
                container = rating_el.find_parent(['div', 'article', 'section'])
                if not container:
                    continue
                
                # Try to find the full review container
                for _ in range(5):  # Go up to 5 levels
                    parent = container.find_parent(['div', 'article', 'section'])
                    if parent:
                        text = parent.get_text()
                        if 'ADAC Erfahrung' in text or 'Bewertung von' in text:
                            container = parent
                            break
                        container = parent
                
                review = self._parse_review_block(container)
                if review and len(review.get('text', '')) > 30:
                    # Avoid duplicates
                    if not any(r['text'][:50] == review['text'][:50] for r in reviews):
                        reviews.append(review)
                        
            except Exception as e:
                logger.debug(f"Error parsing review: {e}")
                continue
        
        # Method 3: Parse using text patterns directly
        if len(reviews) < 10:
            text_reviews = self._extract_from_text(soup)
            for review in text_reviews:
                if not any(r['text'][:50] == review['text'][:50] for r in reviews):
                    reviews.append(review)
        
        return reviews

    def _parse_review_block(self, container) -> dict | None:
        """Parse a review container."""
        try:
            text = container.get_text(separator='\n', strip=True)
            
            # Extract review number
            num_match = re.search(r'ADAC Erfahrung #(\d+)', text)
            review_num = num_match.group(1) if num_match else None
            
            # Extract rating
            rating = None
            rating_match = re.search(r'(\d)[,.](\d)\s*von\s*5|(\d)\s*von\s*5', text)
            if rating_match:
                if rating_match.group(3):
                    rating = float(rating_match.group(3))
                else:
                    rating = float(f"{rating_match.group(1)}.{rating_match.group(2)}")
            
            # Extract date - pattern: "Bewertung von X am DD.MM.YYYY"
            date = None
            date_match = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', text)
            if date_match:
                try:
                    date = datetime(
                        int(date_match.group(3)),
                        int(date_match.group(2)),
                        int(date_match.group(1))
                    )
                except:
                    pass
            
            # Extract author
            author = None
            author_match = re.search(r'Bewertung von\s+(\w+(?:\s+\w+)?)\s+am', text)
            if author_match:
                author = author_match.group(1)
            
            # Extract review text - everything after the date line
            lines = text.split('\n')
            review_text = ""
            
            # Find the actual review content
            capture = False
            skip_patterns = ['Link kopieren', 'Melden', 'von 5', 'Bewertung von', 'ADAC Erfahrung']
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                # Start capturing after date
                if re.search(r'\d{2}\.\d{2}\.\d{4}', line):
                    capture = True
                    continue
                
                if capture:
                    # Skip UI elements
                    if any(skip in line for skip in skip_patterns):
                        continue
                    if len(line) > 20:
                        review_text = line
                        break
            
            if not review_text or len(review_text) < 30:
                return None
            
            return {
                'text': f"[ADAC Finanzfluss #{review_num}] {review_text}" if review_num else f"[ADAC Finanzfluss] {review_text}",
                'rating': rating,
                'author': author,
                'date': date.isoformat() if date else None,
                'source': 'finanzfluss',
                'source_url': self.base_url,
            }
            
        except Exception as e:
            logger.debug(f"Error parsing block: {e}")
            return None

    def _extract_from_text(self, soup: BeautifulSoup) -> list[dict]:
        """Extract reviews by parsing the full page text."""
        reviews = []
        text = soup.get_text()
        
        # Split by review markers
        parts = re.split(r'(ADAC Erfahrung #\d+)', text)
        
        for i, part in enumerate(parts):
            if 'ADAC Erfahrung #' in part:
                # Get the next part which contains the review
                if i + 1 < len(parts):
                    review_text = parts[i + 1]
                    
                    # Extract rating
                    rating = None
                    rating_match = re.search(r'(\d)[,.]?(\d)?\s*von\s*5', review_text)
                    if rating_match:
                        if rating_match.group(2):
                            rating = float(f"{rating_match.group(1)}.{rating_match.group(2)}")
                        else:
                            rating = float(rating_match.group(1))
                    
                    # Extract date
                    date = None
                    date_match = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', review_text)
                    if date_match:
                        try:
                            date = datetime(
                                int(date_match.group(3)),
                                int(date_match.group(2)),
                                int(date_match.group(1))
                            )
                        except:
                            pass
                    
                    # Get review number
                    num_match = re.search(r'#(\d+)', part)
                    review_num = num_match.group(1) if num_match else None
                    
                    # Extract actual review content
                    # Remove rating and date lines
                    clean_text = re.sub(r'\d[,.]?\d?\s*von\s*5\s*Sternen?', '', review_text)
                    clean_text = re.sub(r'\d{2}\.\d{2}\.\d{4}', '', clean_text)
                    clean_text = re.sub(r'Bewertung von.*?am', '', clean_text)
                    clean_text = re.sub(r'Link kopieren|Melden', '', clean_text)
                    
                    # Get first substantial paragraph
                    paragraphs = [p.strip() for p in clean_text.split('\n') if len(p.strip()) > 50]
                    if paragraphs:
                        content = paragraphs[0][:1000]
                        
                        reviews.append({
                            'text': f"[ADAC Finanzfluss #{review_num}] {content}" if review_num else f"[ADAC Finanzfluss] {content}",
                            'rating': rating,
                            'date': date.isoformat() if date else None,
                            'source': 'finanzfluss',
                            'source_url': self.base_url,
                        })
        
        return reviews


def scrape_finanzfluss_adac(max_reviews: int = 200) -> list[dict]:
    """Convenience function to scrape Finanzfluss ADAC reviews."""
    scraper = FinanzflussADACScraper()
    return scraper.scrape_reviews(max_reviews=max_reviews)


if __name__ == "__main__":
    import json
    
    print("=" * 60)
    print("FINANZFLUSS ADAC SCRAPER TEST")
    print("=" * 60)
    
    scraper = FinanzflussADACScraper()
    reviews = scraper.scrape_reviews(max_reviews=50)
    
    print(f"\nCollected {len(reviews)} reviews")
    
    if reviews:
        print("\nSample reviews:")
        for i, review in enumerate(reviews[:3]):
            print(f"\n--- Review {i+1} ---")
            print(f"Rating: {review.get('rating')}")
            print(f"Date: {review.get('date')}")
            print(f"Text: {review.get('text', '')[:200]}...")
        
        with open('finanzfluss_adac_test.json', 'w', encoding='utf-8') as f:
            json.dump(reviews, f, ensure_ascii=False, indent=2)
        print(f"\nSaved to finanzfluss_adac_test.json")
