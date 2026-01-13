"""
ADAC Review Scraper V5 - FOCUSED EDITION

Konzentriert auf die Quellen die WIRKLICH funktionieren:
1. Trustpilot - ALLE 6,400+ Reviews (fixed pagination)
2. Kununu - Fixed deduplication

Usage:
    python scrape_adac_v5.py
    python scrape_adac_v5.py --max-pages 300
"""

import argparse
import json
import re
import sys
import time
import hashlib
from datetime import datetime, timedelta

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("pip install requests beautifulsoup4 lxml")
    sys.exit(1)


# ============================================================================
# TRUSTPILOT SCRAPER - WIRKLICH ALLE SEITEN
# ============================================================================

class TrustpilotMaxScraper:
    """
    Trustpilot ADAC - ALLE Reviews holen.
    
    Problem vorher: Scraper stoppte nach ~20 Seiten weil empty_count zu schnell triggerte.
    Fix: Mehr Toleranz, bessere Erkennung.
    """
    
    name = "trustpilot"
    base_url = "https://de.trustpilot.com/review/www.adac.de"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Sec-Ch-Ua': '"Not A(Brand";v="99", "Google Chrome";v="121"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
        })
        self.all_seen = set()
    
    def scrape(self, max_pages=300):
        """Scrape ALLE Trustpilot Seiten."""
        all_reviews = []
        consecutive_failures = 0
        last_count = 0
        
        print(f"  Ziel: bis zu {max_pages} Seiten (~{max_pages * 20} Reviews)")
        print()
        
        for page in range(1, max_pages + 1):
            url = f"{self.base_url}?page={page}"
            
            try:
                # Variiere Headers leicht
                self.session.headers['Referer'] = f"{self.base_url}?page={page-1}" if page > 1 else self.base_url
                
                resp = self.session.get(url, timeout=45)
                
                # Check für Ende der Reviews
                if resp.status_code == 404:
                    print(f"  Page {page}: 404 - Ende erreicht")
                    break
                
                if resp.status_code == 403:
                    print(f"  Page {page}: 403 Blocked - Warte 30s...")
                    time.sleep(30)
                    consecutive_failures += 1
                    if consecutive_failures > 5:
                        print("  Zu viele Blocks, stoppe.")
                        break
                    continue
                
                resp.raise_for_status()
                
                # Parse
                page_reviews = self._parse_page(resp.text, page)
                
                if page_reviews:
                    all_reviews.extend(page_reviews)
                    consecutive_failures = 0
                    
                    # Progress
                    if page % 10 == 0:
                        new_reviews = len(all_reviews) - last_count
                        print(f"  Page {page}: +{new_reviews} → Total: {len(all_reviews)}")
                        last_count = len(all_reviews)
                else:
                    consecutive_failures += 1
                    if page % 10 == 0:
                        print(f"  Page {page}: 0 neue Reviews (fails: {consecutive_failures})")
                
                # Stopp-Bedingung: 10 Seiten ohne neue Reviews
                if consecutive_failures >= 10:
                    print(f"  10 Seiten ohne neue Reviews - Ende bei Page {page}")
                    break
                
                # Rate limiting - variabel
                delay = 0.3 + (page % 7) * 0.1
                if page % 50 == 0:
                    delay = 3  # Längere Pause alle 50 Seiten
                time.sleep(delay)
                
            except requests.exceptions.Timeout:
                print(f"  Page {page}: Timeout")
                consecutive_failures += 1
                time.sleep(5)
            except Exception as e:
                print(f"  Page {page}: Error - {str(e)[:50]}")
                consecutive_failures += 1
                time.sleep(2)
        
        print(f"\n  Fertig: {len(all_reviews)} Reviews von {page} Seiten")
        return all_reviews
    
    def _parse_page(self, html: str, page_num: int) -> list:
        """Parse Trustpilot page mit mehreren Methoden."""
        soup = BeautifulSoup(html, 'lxml')
        reviews = []
        
        # === METHODE 1: __NEXT_DATA__ JSON ===
        next_data = soup.find('script', {'id': '__NEXT_DATA__'})
        if next_data:
            try:
                data = json.loads(next_data.string)
                review_list = data.get('props', {}).get('pageProps', {}).get('reviews', [])
                
                for r in review_list:
                    review = self._extract_from_json(r, page_num)
                    if review:
                        reviews.append(review)
                
                if reviews:
                    return reviews
            except:
                pass
        
        # === METHODE 2: data-service-review-* Attribute ===
        rating_els = soup.find_all(attrs={'data-service-review-rating': True})
        
        for rating_el in rating_els:
            review = self._extract_from_dom(rating_el, page_num)
            if review:
                reviews.append(review)
        
        if reviews:
            return reviews
        
        # === METHODE 3: Article-basiert ===
        articles = soup.find_all('article')
        
        for article in articles:
            review = self._extract_from_article(article, page_num)
            if review:
                reviews.append(review)
        
        return reviews
    
    def _extract_from_json(self, r: dict, page_num: int) -> dict | None:
        """Extrahiere Review aus JSON-Daten."""
        try:
            text = r.get('text', '')
            title = r.get('title', '')
            
            if not text and not title:
                return None
            
            full_text = f"{title}\n{text}".strip() if title and text else (title or text)
            
            # Deduplizierung
            text_hash = hashlib.md5(full_text.encode()).hexdigest()[:16]
            if text_hash in self.all_seen:
                return None
            self.all_seen.add(text_hash)
            
            if len(full_text) < 15:
                return None
            
            return {
                'text': f"[ADAC Trustpilot] {full_text}",
                'rating': r.get('rating'),
                'author': r.get('consumer', {}).get('displayName'),
                'date': r.get('dates', {}).get('publishedDate', '')[:10] if r.get('dates') else None,
                'source': 'trustpilot.de',
                'source_url': f"{self.base_url}?page={page_num}",
            }
        except:
            return None
    
    def _extract_from_dom(self, rating_el, page_num: int) -> dict | None:
        """Extrahiere Review aus DOM-Element."""
        try:
            rating = float(rating_el.get('data-service-review-rating', 0))
            
            container = rating_el.find_parent(['article', 'section', 'div'])
            if not container:
                return None
            
            # Title
            title = ""
            title_el = container.find(attrs={'data-service-review-title-typography': True})
            if title_el:
                title = title_el.get_text(strip=True)
            
            # Text
            text = ""
            text_el = container.find(attrs={'data-service-review-text-typography': True})
            if text_el:
                text = text_el.get_text(strip=True)
            
            if not text and not title:
                return None
            
            full_text = f"{title}\n{text}".strip() if title else text
            
            # Deduplizierung
            text_hash = hashlib.md5(full_text.encode()).hexdigest()[:16]
            if text_hash in self.all_seen:
                return None
            self.all_seen.add(text_hash)
            
            if len(full_text) < 15:
                return None
            
            # Author
            author = None
            author_el = container.find(attrs={'data-consumer-name-typography': True})
            if author_el:
                author = author_el.get_text(strip=True)
            
            # Date
            date = None
            time_el = container.find('time')
            if time_el:
                date = time_el.get('datetime', '')[:10]
            
            return {
                'text': f"[ADAC Trustpilot] {full_text}",
                'rating': rating,
                'author': author,
                'date': date,
                'source': 'trustpilot.de',
                'source_url': f"{self.base_url}?page={page_num}",
            }
        except:
            return None
    
    def _extract_from_article(self, article, page_num: int) -> dict | None:
        """Extrahiere Review aus Article-Element."""
        try:
            # Suche Rating
            rating = None
            star_img = article.find('img', {'src': re.compile(r'stars-(\d)')})
            if star_img:
                match = re.search(r'stars-(\d)', star_img.get('src', ''))
                if match:
                    rating = float(match.group(1))
            
            # Suche Text in Paragraphen
            texts = []
            for p in article.find_all('p'):
                p_text = p.get_text(strip=True)
                # Filter UI-Text
                if len(p_text) > 20 and not any(x in p_text.lower() for x in 
                    ['bewertung', 'antwort', 'unternehmen', 'website', 'profil', 'hilfreich']):
                    texts.append(p_text)
            
            if not texts:
                return None
            
            full_text = ' '.join(texts[:2])
            
            # Deduplizierung
            text_hash = hashlib.md5(full_text.encode()).hexdigest()[:16]
            if text_hash in self.all_seen:
                return None
            self.all_seen.add(text_hash)
            
            if len(full_text) < 30:
                return None
            
            return {
                'text': f"[ADAC Trustpilot] {full_text[:600]}",
                'rating': rating,
                'author': None,
                'date': None,
                'source': 'trustpilot.de',
                'source_url': f"{self.base_url}?page={page_num}",
            }
        except:
            return None


# ============================================================================
# KUNUNU SCRAPER - FIXED DEDUPLICATION
# ============================================================================

class KununuFixedScraper:
    """
    Kununu ADAC - Bessere Text-Extraktion ohne Duplikate.
    
    Problem vorher: Container-Text enthielt Header/Footer → Duplikate
    Fix: Nur spezifische Inhalts-Elemente extrahieren
    """
    
    name = "kununu"
    base_url = "https://www.kununu.com/de/adac/kommentare"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'de-DE,de;q=0.9',
        })
        self.seen_hashes = set()
    
    def scrape(self, max_pages=60):
        """Scrape Kununu mit besserer Deduplizierung."""
        all_reviews = []
        empty_streak = 0
        
        for page in range(1, max_pages + 1):
            url = f"{self.base_url}?page={page}" if page > 1 else self.base_url
            
            try:
                resp = self.session.get(url, timeout=30)
                
                if resp.status_code == 404:
                    break
                
                resp.raise_for_status()
                page_reviews = self._parse_page(resp.text, page)
                
                if page_reviews:
                    all_reviews.extend(page_reviews)
                    empty_streak = 0
                    
                    if page % 10 == 0:
                        print(f"  Page {page}: {len(all_reviews)} total")
                else:
                    empty_streak += 1
                    if empty_streak >= 5:
                        break
                
                time.sleep(0.6)
                
            except Exception as e:
                if "404" in str(e):
                    break
                continue
        
        print(f"  Fertig: {len(all_reviews)} unique Reviews")
        return all_reviews
    
    def _parse_page(self, html: str, page_num: int) -> list:
        """Parse Kununu mit spezifischer Element-Extraktion."""
        soup = BeautifulSoup(html, 'lxml')
        reviews = []
        
        # Finde Review-Container anhand von Index-Pattern
        # Kununu nutzt index-review-X als ID
        for i in range(50):  # Max 50 Reviews pro Seite
            review_id = f"index-review-{i}"
            container = soup.find(id=review_id)
            
            if not container:
                # Alternative: data-testid
                container = soup.find(attrs={'data-testid': f'review-{i}'})
            
            if not container:
                continue
            
            review = self._extract_review(container, page_num)
            if review:
                reviews.append(review)
        
        # Fallback: Alle article-Elemente
        if not reviews:
            for article in soup.find_all('article'):
                # Prüfe ob es wirklich ein Review ist
                if article.find(['h2', 'h3']) or article.find(string=re.compile(r'Pro|Contra|Gut|Schlecht', re.I)):
                    review = self._extract_review(article, page_num)
                    if review:
                        reviews.append(review)
        
        return reviews
    
    def _extract_review(self, container, page_num: int) -> dict | None:
        """Extrahiere einzelnen Review."""
        try:
            parts = []
            
            # 1. Headline/Titel
            headline = container.find(['h2', 'h3', 'h4'])
            if headline:
                title = headline.get_text(strip=True)
                if title and len(title) > 5 and len(title) < 200:
                    parts.append(f"Titel: {title}")
            
            # 2. Pro/Contra/Verbesserung - SPEZIFISCH suchen
            categories = {
                'Gut am Arbeitgeber': 'Pro',
                'Schlecht am Arbeitgeber': 'Contra',
                'Verbesserungsvorschläge': 'Verbesserung',
                'Work-Life-Balance': 'Work-Life',
                'Gehalt/Sozialleistungen': 'Gehalt',
                'Karriere/Weiterbildung': 'Karriere',
                'Arbeitsatmosphäre': 'Atmosphäre',
                'Kollegenzusammenhalt': 'Kollegen',
                'Vorgesetztenverhalten': 'Vorgesetzte',
            }
            
            for search_term, label in categories.items():
                # Finde Label-Element
                label_el = container.find(string=re.compile(search_term, re.I))
                if label_el:
                    # Finde den zugehörigen Text (nächstes Geschwister oder Parent)
                    parent = label_el.parent
                    if parent:
                        # Suche nächstes Text-Element
                        for sibling in parent.find_next_siblings(['p', 'div', 'span'])[:2]:
                            text = sibling.get_text(strip=True)
                            if text and len(text) > 10 and text not in [p.split(': ', 1)[-1] for p in parts]:
                                parts.append(f"{label}: {text}")
                                break
            
            # 3. Wenn keine strukturierten Daten, nimm alle <p> Tags
            if len(parts) <= 1:
                for p in container.find_all('p', recursive=True):
                    p_text = p.get_text(strip=True)
                    if len(p_text) > 30 and p_text not in [p.split(': ', 1)[-1] for p in parts]:
                        # Prüfe ob nicht UI-Text
                        if not any(ui in p_text.lower() for ui in ['bewertung', 'melden', 'hilfreich', 'anmelden']):
                            parts.append(p_text)
                            if len(parts) >= 5:
                                break
            
            if not parts:
                return None
            
            full_text = ' | '.join(parts)
            
            # Deduplizierung mit Hash
            text_hash = hashlib.md5(full_text[:200].encode()).hexdigest()[:16]
            if text_hash in self.seen_hashes:
                return None
            self.seen_hashes.add(text_hash)
            
            if len(full_text) < 50:
                return None
            
            # Rating
            rating = None
            # Suche Score-Element
            score_patterns = [
                r'(\d[,.]?\d?)\s*/\s*5',
                r'Score:\s*(\d[,.]?\d?)',
            ]
            container_text = container.get_text()
            for pattern in score_patterns:
                match = re.search(pattern, container_text)
                if match:
                    try:
                        rating = float(match.group(1).replace(',', '.'))
                        break
                    except:
                        pass
            
            return {
                'text': f"[ADAC Kununu] {full_text[:800]}",
                'rating': rating,
                'author': None,
                'date': None,
                'source': 'kununu.com',
                'source_url': f"{self.base_url}?page={page_num}",
            }
            
        except Exception as e:
            return None


# ============================================================================
# APPLE APP STORE - FIXED
# ============================================================================

class AppStoreFixedScraper:
    """
    Apple App Store ADAC Apps.
    
    Nutzt die iTunes Search API für Reviews.
    """
    
    name = "appstore"
    
    # Korrekte App IDs (aus App Store Links)
    APPS = [
        ("ADAC", "410683848"),
        ("ADAC Spritpreise", "450498498"),
        ("ADAC Camping", "498457072"),
        ("ADAC Maps", "921967184"),
        ("ADAC Pannenhilfe", "1473866498"),
    ]
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15',
            'Accept': '*/*',
        })
    
    def scrape(self):
        """Scrape App Store Reviews."""
        all_reviews = []
        
        for app_name, app_id in self.APPS:
            print(f"  {app_name}...", end=" ", flush=True)
            
            # Versuche verschiedene API-Endpunkte
            urls = [
                f"https://itunes.apple.com/de/rss/customerreviews/id={app_id}/sortBy=mostRecent/json",
                f"https://itunes.apple.com/de/rss/customerreviews/page=1/id={app_id}/sortby=mostrecent/json",
                f"https://itunes.apple.com/rss/customerreviews/id={app_id}/json",
            ]
            
            reviews = []
            for url in urls:
                try:
                    resp = self.session.get(url, timeout=30)
                    if resp.status_code == 200:
                        data = resp.json()
                        entries = data.get('feed', {}).get('entry', [])
                        
                        # Erste Entry ist App-Info, Rest sind Reviews
                        for entry in entries:
                            if not isinstance(entry, dict):
                                continue
                            
                            content = entry.get('content', {})
                            if isinstance(content, dict):
                                text = content.get('label', '')
                            else:
                                continue
                            
                            if not text or len(text) < 10:
                                continue
                            
                            title = entry.get('title', {}).get('label', '')
                            rating = entry.get('im:rating', {}).get('label')
                            author = entry.get('author', {}).get('name', {}).get('label')
                            
                            full_text = f"{title}\n{text}" if title else text
                            
                            reviews.append({
                                'text': f"[{app_name} iOS] {full_text[:500]}",
                                'rating': float(rating) if rating else None,
                                'author': author,
                                'date': None,
                                'source': 'apps.apple.com',
                                'source_url': f"https://apps.apple.com/de/app/id{app_id}",
                            })
                        
                        if reviews:
                            break
                            
                except Exception as e:
                    continue
            
            all_reviews.extend(reviews)
            print(f"{len(reviews)} reviews")
            time.sleep(0.5)
        
        return all_reviews


# ============================================================================
# FINANZFLUSS - Mit allen Seiten
# ============================================================================

class FinanzflussMaxScraper:
    """Finanzfluss - Scrape alle Seiten."""
    
    name = "finanzfluss"
    base_url = "https://www.finanzfluss.de/anbieter/adac/erfahrungen/"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'de-DE,de;q=0.9',
        })
    
    def scrape(self, max_pages=20):
        """Scrape alle Finanzfluss Seiten."""
        all_reviews = []
        
        for page in range(1, max_pages + 1):
            url = f"{self.base_url}?page={page}" if page > 1 else self.base_url
            
            try:
                resp = self.session.get(url, timeout=30)
                
                if resp.status_code == 404:
                    break
                
                resp.raise_for_status()
                
                reviews = self._parse_page(resp.text, page)
                
                if not reviews and page > 1:
                    break
                
                all_reviews.extend(reviews)
                
                if page == 1:
                    print(f"  Page 1: {len(reviews)} reviews", end="")
                
                time.sleep(0.5)
                
            except Exception as e:
                break
        
        print(f" → Total: {len(all_reviews)}")
        return all_reviews
    
    def _parse_page(self, html: str, page_num: int) -> list:
        """Parse Finanzfluss Seite."""
        soup = BeautifulSoup(html, 'lxml')
        reviews = []
        seen = set()
        
        # JSON-LD
        for script in soup.find_all('script', {'type': 'application/ld+json'}):
            try:
                data = json.loads(script.string)
                if isinstance(data, dict):
                    for r in data.get('review', []):
                        text = r.get('reviewBody', '')
                        if text and len(text) > 20 and text[:50] not in seen:
                            seen.add(text[:50])
                            reviews.append({
                                'text': f"[ADAC Finanzfluss] {text[:600]}",
                                'rating': r.get('reviewRating', {}).get('ratingValue'),
                                'author': r.get('author', {}).get('name') if isinstance(r.get('author'), dict) else None,
                                'date': r.get('datePublished'),
                                'source': 'finanzfluss.de',
                                'source_url': self.base_url,
                            })
            except:
                pass
        
        return reviews


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="ADAC Review Scraper V5")
    parser.add_argument('--max-pages', type=int, default=350,
                       help='Max Trustpilot Seiten (default: 350 = ~7000 Reviews)')
    parser.add_argument('--output', type=str, default='adac_reviews_v5.json')
    parser.add_argument('--only', choices=['trustpilot', 'kununu', 'appstore', 'finanzfluss'],
                       help='Nur diese Quelle scrapen')
    
    args = parser.parse_args()
    
    print("=" * 70)
    print(" ADAC REVIEW SCRAPER V5 - FOCUSED EDITION")
    print("=" * 70)
    print(f" Ziel: Trustpilot {args.max_pages} Seiten")
    print(f" Output: {args.output}")
    print("=" * 70)
    
    all_reviews = []
    stats = {}
    
    # Trustpilot
    if not args.only or args.only == 'trustpilot':
        print(f"\n{'=' * 70}")
        print(" TRUSTPILOT (~6,400 Reviews verfügbar)")
        print("=" * 70)
        scraper = TrustpilotMaxScraper()
        reviews = scraper.scrape(max_pages=args.max_pages)
        all_reviews.extend(reviews)
        stats['trustpilot'] = len(reviews)
    
    # Kununu
    if not args.only or args.only == 'kununu':
        print(f"\n{'=' * 70}")
        print(" KUNUNU (Arbeitgeber-Bewertungen)")
        print("=" * 70)
        scraper = KununuFixedScraper()
        reviews = scraper.scrape(max_pages=60)
        all_reviews.extend(reviews)
        stats['kununu'] = len(reviews)
    
    # App Store
    if not args.only or args.only == 'appstore':
        print(f"\n{'=' * 70}")
        print(" APPLE APP STORE (ADAC Apps)")
        print("=" * 70)
        scraper = AppStoreFixedScraper()
        reviews = scraper.scrape()
        all_reviews.extend(reviews)
        stats['appstore'] = len(reviews)
    
    # Finanzfluss
    if not args.only or args.only == 'finanzfluss':
        print(f"\n{'=' * 70}")
        print(" FINANZFLUSS")
        print("=" * 70)
        scraper = FinanzflussMaxScraper()
        reviews = scraper.scrape()
        all_reviews.extend(reviews)
        stats['finanzfluss'] = len(reviews)
    
    # === ERGEBNIS ===
    print(f"\n{'=' * 70}")
    print(" ERGEBNIS")
    print("=" * 70)
    
    if all_reviews:
        # Finale Deduplizierung
        final_seen = set()
        unique_reviews = []
        for r in all_reviews:
            h = hashlib.md5(r['text'][:150].encode()).hexdigest()[:16]
            if h not in final_seen:
                final_seen.add(h)
                unique_reviews.append(r)
        
        # Save
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(unique_reviews, f, ensure_ascii=False, indent=2)
        
        training_file = args.output.replace('.json', '_training.json')
        training_data = [{'id': i+1, 'text': r['text']} for i, r in enumerate(unique_reviews)]
        with open(training_file, 'w', encoding='utf-8') as f:
            json.dump(training_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n  Roh: {len(all_reviews)}")
        print(f"  Unique: {len(unique_reviews)}")
        print()
        print("  Statistik:")
        for src, cnt in stats.items():
            print(f"    • {src}: {cnt}")
        print()
        print(f"  ✅ TOTAL: {len(unique_reviews)} Reviews!")
        print(f"  📁 {args.output}")
        print(f"  📁 {training_file}")
        
        print("\n  Beispiele:")
        for i, r in enumerate(unique_reviews[:3]):
            rating = f"★{r['rating']}" if r.get('rating') else "☆"
            print(f"  [{i+1}] {rating} {r['text'][:60]}...")
    else:
        print("  ❌ Keine Reviews!")


if __name__ == "__main__":
    main()