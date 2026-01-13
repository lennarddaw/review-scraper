"""
Trustpilot ADAC Scraper - VERIFIED WORKING

Scrapes ADAC reviews from Trustpilot.de
Source: https://de.trustpilot.com/review/www.adac.de
Total available: 6,368+ reviews

Based on actual HTML analysis from January 2026.
"""

import re
import time
from datetime import datetime, timedelta
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from loguru import logger


class TrustpilotADACScraper:
    """Scraper for Trustpilot ADAC reviews."""

    name = "trustpilot"
    base_url = "https://de.trustpilot.com/review/www.adac.de"
    
    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })

    def scrape_reviews(self, max_reviews: int = 1000, max_pages: int = 50) -> list[dict]:
        """
        Scrape ADAC reviews from Trustpilot.
        
        Args:
            max_reviews: Maximum number of reviews to collect
            max_pages: Maximum pages to scrape (20 reviews per page)
        """
        all_reviews = []
        page = 1
        
        while len(all_reviews) < max_reviews and page <= max_pages:
            try:
                url = f"{self.base_url}?page={page}" if page > 1 else self.base_url
                logger.info(f"[{self.name}] Fetching page {page}: {url}")
                
                response = self._session.get(url, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Find all review cards - multiple selector strategies
                reviews_on_page = self._extract_reviews_from_page(soup, url)
                
                if not reviews_on_page:
                    logger.warning(f"[{self.name}] No reviews found on page {page}")
                    # Try alternative extraction
                    reviews_on_page = self._extract_reviews_alternative(soup, url)
                
                if not reviews_on_page:
                    logger.info(f"[{self.name}] No more reviews, stopping at page {page}")
                    break
                
                all_reviews.extend(reviews_on_page)
                logger.info(f"[{self.name}] Page {page}: {len(reviews_on_page)} reviews (total: {len(all_reviews)})")
                
                page += 1
                time.sleep(1.5)  # Be respectful
                
            except Exception as e:
                logger.error(f"[{self.name}] Error on page {page}: {e}")
                break
        
        return all_reviews[:max_reviews]

    def _extract_reviews_from_page(self, soup: BeautifulSoup, source_url: str) -> list[dict]:
        """Extract reviews using primary selectors."""
        reviews = []
        
        # Trustpilot review cards - look for review article elements
        # Based on observed structure: each review has star rating image and text
        
        # Strategy 1: Find by star rating images (reliable indicator of review)
        star_imgs = soup.find_all('img', src=lambda x: x and 'stars-' in str(x))
        
        for star_img in star_imgs:
            try:
                # Navigate up to find the review container
                container = star_img.find_parent('article') or star_img.find_parent('div', class_=lambda x: x and 'review' in str(x).lower())
                
                if not container:
                    # Try finding parent with substantial text
                    for parent in star_img.parents:
                        text = parent.get_text(strip=True)
                        if len(text) > 100:
                            container = parent
                            break
                
                if not container:
                    continue
                
                review = self._parse_review_container(container, source_url)
                if review and review.get('text') and len(review['text']) > 20:
                    # Avoid duplicates
                    if not any(r['text'][:50] == review['text'][:50] for r in reviews):
                        reviews.append(review)
                        
            except Exception as e:
                logger.debug(f"Error parsing review: {e}")
                continue
        
        return reviews

    def _extract_reviews_alternative(self, soup: BeautifulSoup, source_url: str) -> list[dict]:
        """Alternative extraction method using text patterns."""
        reviews = []
        
        # Find all text blocks that look like reviews
        # Trustpilot reviews often have user links like /users/...
        user_links = soup.find_all('a', href=lambda x: x and '/users/' in str(x))
        
        for user_link in user_links:
            try:
                # Find the review container around this user
                container = None
                for parent in user_link.parents:
                    # Look for a container with review-like content
                    text = parent.get_text(strip=True)
                    if len(text) > 100 and len(text) < 5000:
                        # Check if it has rating indicators
                        if 'Stern' in text or 'star' in text.lower() or any(c in text for c in '★☆⭐'):
                            container = parent
                            break
                
                if container:
                    review = self._parse_review_container(container, source_url)
                    if review and review.get('text') and len(review['text']) > 20:
                        if not any(r['text'][:50] == review['text'][:50] for r in reviews):
                            reviews.append(review)
                            
            except Exception as e:
                continue
        
        return reviews

    def _parse_review_container(self, container, source_url: str) -> dict | None:
        """Parse a review container element."""
        try:
            # Get all text content
            full_text = container.get_text(separator=' ', strip=True)
            
            # Clean up the text - remove navigation/UI elements
            skip_phrases = [
                'Bewertung abgeben', 'Zur Website', 'Mehr ansehen', 
                'Nächste Seite', 'Zurück', 'Alle Bewertungen',
                'Unternehmen hat geantwortet', 'Link kopieren'
            ]
            
            # Extract the actual review text
            review_text = self._extract_review_text(container)
            
            if not review_text or len(review_text) < 20:
                return None
            
            # Skip if it's clearly not a review
            if any(phrase in review_text for phrase in skip_phrases):
                return None
            
            # Extract rating from star images or aria-labels
            rating = self._extract_rating(container)
            
            # Extract author
            author = self._extract_author(container)
            
            # Extract date
            date = self._extract_date(container)
            
            return {
                'text': f"[ADAC Trustpilot] {review_text}",
                'rating': rating,
                'author': author,
                'date': date.isoformat() if date else None,
                'source': 'trustpilot',
                'source_url': source_url,
            }
            
        except Exception as e:
            logger.debug(f"Error parsing container: {e}")
            return None

    def _extract_review_text(self, container) -> str:
        """Extract the actual review text from container."""
        # Look for specific review text elements
        text_candidates = []
        
        # Method 1: Find paragraphs
        for p in container.find_all(['p', 'span', 'div']):
            text = p.get_text(strip=True)
            # Filter out short texts and UI elements
            if len(text) > 30 and not any(skip in text for skip in ['Bewertung', 'Stern', 'Sterne', 'von 5']):
                text_candidates.append(text)
        
        # Method 2: Get text after rating section
        full_text = container.get_text(separator='\n', strip=True)
        lines = [l.strip() for l in full_text.split('\n') if l.strip()]
        
        # Find the first substantial line that's not metadata
        for line in lines:
            if len(line) > 50 and not any(skip in line for skip in [
                'Bewertung', 'Sterne', 'von 5', 'Mehr ansehen', 
                'Unternehmen', 'Link kopieren', 'Melden'
            ]):
                return line[:1000]  # Limit length
        
        # Fallback to longest candidate
        if text_candidates:
            return max(text_candidates, key=len)[:1000]
        
        return ""

    def _extract_rating(self, container) -> float | None:
        """Extract rating from container."""
        # Method 1: Star image URL
        star_img = container.find('img', src=lambda x: x and 'stars-' in str(x))
        if star_img:
            src = star_img.get('src', '')
            match = re.search(r'stars-(\d)', src)
            if match:
                return float(match.group(1))
        
        # Method 2: Aria-label
        for el in container.find_all(attrs={'aria-label': True}):
            label = el.get('aria-label', '')
            match = re.search(r'(\d)[,.]?\d?\s*(?:von|of)\s*5', label)
            if match:
                return float(match.group(1))
            match = re.search(r'Bewertet mit (\d)', label)
            if match:
                return float(match.group(1))
        
        # Method 3: Text content
        text = container.get_text()
        match = re.search(r'(\d)\s*(?:von|of)\s*5\s*Stern', text)
        if match:
            return float(match.group(1))
        
        return None

    def _extract_author(self, container) -> str | None:
        """Extract author name."""
        # Find user link
        user_link = container.find('a', href=lambda x: x and '/users/' in str(x))
        if user_link:
            return user_link.get_text(strip=True)
        
        return None

    def _extract_date(self, container) -> datetime | None:
        """Extract review date."""
        text = container.get_text()
        
        # German date patterns
        patterns = [
            # "Vor X Tagen"
            (r'Vor\s+(\d+)\s+Tag', lambda m: datetime.now() - timedelta(days=int(m.group(1)))),
            # "Vor X Stunden"
            (r'Vor\s+(\d+)\s+Stund', lambda m: datetime.now() - timedelta(hours=int(m.group(1)))),
            # "X. Jan. 2026"
            (r'(\d{1,2})\.\s*(Jan|Feb|Mär|Apr|Mai|Jun|Jul|Aug|Sep|Okt|Nov|Dez)\w*\.?\s*(\d{4})', self._parse_german_date),
            # "Aktualisiert am X. Jan. 2026"
            (r'Aktualisiert.*?(\d{1,2})\.\s*(Jan|Feb|Mär|Apr|Mai|Jun|Jul|Aug|Sep|Okt|Nov|Dez)\w*\.?\s*(\d{4})', self._parse_german_date),
        ]
        
        for pattern, parser in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    return parser(match)
                except:
                    continue
        
        return None

    def _parse_german_date(self, match) -> datetime:
        """Parse German date match."""
        months = {
            'jan': 1, 'feb': 2, 'mär': 3, 'mar': 3, 'apr': 4,
            'mai': 5, 'jun': 6, 'jul': 7, 'aug': 8,
            'sep': 9, 'okt': 10, 'nov': 11, 'dez': 12
        }
        
        day = int(match.group(1))
        month_str = match.group(2).lower()[:3]
        year = int(match.group(3))
        month = months.get(month_str, 1)
        
        return datetime(year, month, day)


def scrape_trustpilot_adac(max_reviews: int = 500) -> list[dict]:
    """Convenience function to scrape Trustpilot ADAC reviews."""
    scraper = TrustpilotADACScraper()
    return scraper.scrape_reviews(max_reviews=max_reviews)


if __name__ == "__main__":
    # Test the scraper
    import json
    
    print("=" * 60)
    print("TRUSTPILOT ADAC SCRAPER TEST")
    print("=" * 60)
    
    scraper = TrustpilotADACScraper()
    reviews = scraper.scrape_reviews(max_reviews=50, max_pages=3)
    
    print(f"\nCollected {len(reviews)} reviews")
    
    if reviews:
        print("\nSample reviews:")
        for i, review in enumerate(reviews[:3]):
            print(f"\n--- Review {i+1} ---")
            print(f"Rating: {review.get('rating')}")
            print(f"Author: {review.get('author')}")
            print(f"Date: {review.get('date')}")
            print(f"Text: {review.get('text', '')[:200]}...")
        
        # Save to file
        with open('trustpilot_adac_test.json', 'w', encoding='utf-8') as f:
            json.dump(reviews, f, ensure_ascii=False, indent=2)
        print(f"\nSaved to trustpilot_adac_test.json")
