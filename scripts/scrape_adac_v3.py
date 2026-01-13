"""
ADAC Review Scraper V3 - STANDALONE EDITION (Windows)

Dieses Skript läuft auf deinem Windows PC und scrapt:
- Trustpilot.de (~6,400 ADAC Reviews)
- Finanzfluss.de (~150 Erfahrungen)  
- Kununu.com (Arbeitgeber-Bewertungen)
- Google Play Store (App Reviews)

Die Reviews werden SAUBER extrahiert (ohne Header/Navigation-Text).

Installation:
    pip install requests beautifulsoup4 lxml

Usage:
    python scrape_adac_v3.py
    python scrape_adac_v3.py --max-pages 100
    python scrape_adac_v3.py --sources trustpilot kununu
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Bitte installiere: pip install requests beautifulsoup4 lxml")
    sys.exit(1)


# ============================================================================
# TRUSTPILOT SCRAPER - VERBESSERT
# ============================================================================

class TrustpilotScraper:
    """
    Trustpilot ADAC Scraper.
    
    Bekannte Review-Anzahl: ~6,400 Reviews
    
    HTML Struktur (Stand Jan 2026):
    - Reviews sind in <article> Tags
    - Rating: data-service-review-rating="X"
    - Titel: data-service-review-title-typography  
    - Text: data-service-review-text-typography
    - Author: data-consumer-name-typography
    """
    
    name = "trustpilot"
    base_url = "https://de.trustpilot.com/review/www.adac.de"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
    
    def scrape(self, max_pages=100):
        """Scrape alle ADAC Reviews von Trustpilot."""
        all_reviews = []
        consecutive_empty = 0
        
        for page in range(1, max_pages + 1):
            url = f"{self.base_url}?page={page}"
            print(f"  Page {page}...", end=" ", flush=True)
            
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                
                reviews = self._parse_page(resp.text, page)
                
                if not reviews:
                    consecutive_empty += 1
                    print(f"0 reviews (empty: {consecutive_empty})")
                    if consecutive_empty >= 3:
                        print("  → Keine weiteren Reviews gefunden")
                        break
                    continue
                
                consecutive_empty = 0
                all_reviews.extend(reviews)
                print(f"{len(reviews)} reviews (total: {len(all_reviews)})")
                
                # Rate limiting
                time.sleep(0.8 + (page % 5) * 0.1)
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    print("Page not found - Ende erreicht")
                    break
                print(f"HTTP Error: {e}")
                break
            except Exception as e:
                print(f"Error: {e}")
                if "blocked" in str(e).lower() or "403" in str(e):
                    print("  → Möglicherweise IP blockiert. Warte 30 Sekunden...")
                    time.sleep(30)
                continue
        
        return all_reviews
    
    def _parse_page(self, html: str, page_num: int) -> list:
        """Parse einzelne Seite und extrahiere Reviews."""
        soup = BeautifulSoup(html, 'lxml')
        reviews = []
        seen_texts = set()
        
        # === METHODE 1: Suche nach data-service-review-* Attributen ===
        
        # Finde alle Rating-Container
        rating_containers = soup.find_all(attrs={'data-service-review-rating': True})
        
        for rating_el in rating_containers:
            try:
                # Rating extrahieren
                rating_str = rating_el.get('data-service-review-rating', '')
                rating = float(rating_str) if rating_str else None
                
                # Finde den Parent-Container (meist article oder section)
                container = rating_el.find_parent(['article', 'section'])
                if not container:
                    container = rating_el.find_parent('div')
                
                if not container:
                    continue
                
                # Titel finden (SPEZIFISCH)
                title = ""
                title_el = container.find(attrs={'data-service-review-title-typography': True})
                if title_el:
                    title = title_el.get_text(strip=True)
                
                # Review-Text finden (SPEZIFISCH)
                text = ""
                text_el = container.find(attrs={'data-service-review-text-typography': True})
                if text_el:
                    text = text_el.get_text(strip=True)
                
                if not text and not title:
                    continue
                
                # Kombinierten Text erstellen
                full_text = f"{title}\n{text}".strip() if title and text else (title or text)
                
                # Deduplizierung
                text_hash = full_text[:80].lower()
                if text_hash in seen_texts or len(full_text) < 20:
                    continue
                seen_texts.add(text_hash)
                
                # Author
                author = None
                author_el = container.find(attrs={'data-consumer-name-typography': True})
                if author_el:
                    author = author_el.get_text(strip=True)
                
                # Datum
                date = self._extract_date(container)
                
                reviews.append({
                    'text': f"[ADAC Trustpilot] {full_text}",
                    'rating': rating,
                    'author': author,
                    'date': date,
                    'source': 'trustpilot.de',
                    'source_url': f"{self.base_url}?page={page_num}",
                })
                
            except Exception:
                continue
        
        # === METHODE 2: Fallback - Suche nach Star-Bildern ===
        if not reviews:
            reviews = self._parse_by_stars(soup, page_num, seen_texts)
        
        return reviews
    
    def _parse_by_stars(self, soup, page_num: int, seen_texts: set) -> list:
        """Fallback: Parse nach Star-Rating-Bildern."""
        reviews = []
        
        # Suche Star-Bilder
        star_imgs = soup.find_all('img', {'alt': re.compile(r'Bewertet mit \d')})
        
        for star_img in star_imgs:
            try:
                # Rating aus Alt-Text
                alt = star_img.get('alt', '')
                rating_match = re.search(r'(\d)', alt)
                rating = float(rating_match.group(1)) if rating_match else None
                
                # Finde Container
                container = star_img
                for _ in range(6):  # Max 6 Ebenen hoch
                    parent = container.parent
                    if parent is None:
                        break
                    
                    # Prüfe ob es ein Review-Container ist
                    if parent.name in ['article', 'section']:
                        container = parent
                        break
                    
                    # Oder eine Div mit Review-Attributen
                    if parent.get('data-service-review-rating'):
                        container = parent
                        break
                    
                    container = parent
                
                # Suche Review-Text im Container
                # WICHTIG: Nur spezifische Elemente, nicht den ganzen Container-Text!
                text = ""
                
                # Priorität 1: data-service-review-text-typography
                text_el = container.find(attrs={'data-service-review-text-typography': True})
                if text_el:
                    text = text_el.get_text(strip=True)
                
                # Priorität 2: p Tag innerhalb des Containers (aber nicht alle!)
                if not text:
                    # Finde den relevanten p-Tag (nach dem Titel)
                    paragraphs = container.find_all('p', recursive=True)
                    for p in paragraphs:
                        p_text = p.get_text(strip=True)
                        # Filter: Mind. 30 Zeichen, kein UI-Text
                        if len(p_text) >= 30 and not any(ui in p_text for ui in ['Bewertung', 'Profil', 'Website', 'Antwort']):
                            text = p_text
                            break
                
                if not text or len(text) < 30:
                    continue
                
                # Deduplizierung
                text_hash = text[:80].lower()
                if text_hash in seen_texts:
                    continue
                seen_texts.add(text_hash)
                
                reviews.append({
                    'text': f"[ADAC Trustpilot] {text}",
                    'rating': rating,
                    'author': None,
                    'date': None,
                    'source': 'trustpilot.de',
                    'source_url': f"{self.base_url}?page={page_num}",
                })
                
            except Exception:
                continue
        
        return reviews
    
    def _extract_date(self, container) -> str | None:
        """Extrahiere Datum aus Container."""
        # Suche time Element
        time_el = container.find('time')
        if time_el:
            dt = time_el.get('datetime', '')
            if dt:
                return dt[:10]
        
        text = container.get_text()
        
        # "Vor X Tagen"
        match = re.search(r'Vor\s+(\d+)\s+Tag', text)
        if match:
            days = int(match.group(1))
            return (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        # "Vor X Stunden"
        match = re.search(r'Vor\s+(\d+)\s+Stund', text)
        if match:
            return datetime.now().strftime('%Y-%m-%d')
        
        # "X. Jan. 2026"
        months = {'jan': 1, 'feb': 2, 'mär': 3, 'apr': 4, 'mai': 5, 'jun': 6,
                  'jul': 7, 'aug': 8, 'sep': 9, 'okt': 10, 'nov': 11, 'dez': 12}
        match = re.search(r'(\d{1,2})\.\s*(Jan|Feb|Mär|Apr|Mai|Jun|Jul|Aug|Sep|Okt|Nov|Dez)\w*\.?\s*(\d{4})', text, re.I)
        if match:
            day = int(match.group(1))
            month = months.get(match.group(2).lower()[:3], 1)
            year = int(match.group(3))
            return f"{year}-{month:02d}-{day:02d}"
        
        return None


# ============================================================================
# KUNUNU SCRAPER - ARBEITGEBER BEWERTUNGEN
# ============================================================================

class KununuScraper:
    """
    Kununu ADAC Scraper - Mitarbeiter-Bewertungen.
    
    Liefert Einblicke aus Arbeitnehmer-Perspektive.
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
    
    def scrape(self, max_pages=30):
        """Scrape ADAC Arbeitgeber-Bewertungen."""
        all_reviews = []
        
        for page in range(1, max_pages + 1):
            url = f"{self.base_url}?page={page}" if page > 1 else self.base_url
            print(f"  Page {page}...", end=" ", flush=True)
            
            try:
                resp = self.session.get(url, timeout=30)
                
                if resp.status_code == 404:
                    print("Ende erreicht")
                    break
                    
                resp.raise_for_status()
                reviews = self._parse_page(resp.text, page)
                
                if not reviews:
                    print("0 reviews")
                    break
                
                all_reviews.extend(reviews)
                print(f"{len(reviews)} reviews (total: {len(all_reviews)})")
                
                time.sleep(1.0)
                
            except Exception as e:
                print(f"Error: {e}")
                break
        
        return all_reviews
    
    def _parse_page(self, html: str, page_num: int) -> list:
        """Parse Kununu Seite."""
        soup = BeautifulSoup(html, 'lxml')
        reviews = []
        
        # Kununu verwendet index-review-* IDs
        review_sections = soup.find_all('article')
        
        # Alternative: Suche nach Review-Containern
        if not review_sections:
            review_sections = soup.find_all('div', {'class': re.compile(r'review', re.I)})
        
        for section in review_sections:
            try:
                # Titel
                title = ""
                title_el = section.find(['h2', 'h3', 'h4'])
                if title_el:
                    title = title_el.get_text(strip=True)
                
                # Pro/Contra/Verbesserung Texte sammeln
                texts = []
                
                # Suche nach Textblöcken
                for label in ['Gut am Arbeitgeber', 'Schlecht am Arbeitgeber', 'Pro', 'Contra', 'Verbesserung']:
                    label_el = section.find(string=re.compile(label, re.I))
                    if label_el:
                        # Finde das nächste Text-Element
                        next_el = label_el.find_next(['p', 'div', 'span'])
                        if next_el:
                            text = next_el.get_text(strip=True)
                            if text and len(text) > 10:
                                texts.append(f"{label}: {text}")
                
                # Oder einfach alle p-Tags
                if not texts:
                    paragraphs = section.find_all('p')
                    for p in paragraphs:
                        text = p.get_text(strip=True)
                        if len(text) > 30:
                            texts.append(text)
                
                if not texts and not title:
                    continue
                
                full_text = f"{title}\n" + "\n".join(texts) if title else "\n".join(texts)
                
                if len(full_text.strip()) < 30:
                    continue
                
                # Rating
                rating = None
                rating_el = section.find(['span', 'div'], string=re.compile(r'^\d[,.]?\d?$'))
                if rating_el:
                    rating_text = rating_el.get_text(strip=True)
                    try:
                        rating = float(rating_text.replace(',', '.'))
                    except:
                        pass
                
                reviews.append({
                    'text': f"[ADAC Kununu] {full_text[:800]}",
                    'rating': rating,
                    'author': None,
                    'date': None,
                    'source': 'kununu.com',
                    'source_url': f"{self.base_url}?page={page_num}",
                })
                
            except Exception:
                continue
        
        return reviews


# ============================================================================
# FINANZFLUSS SCRAPER
# ============================================================================

class FinanzflussScraper:
    """Finanzfluss ADAC Erfahrungen."""
    
    name = "finanzfluss"
    base_url = "https://www.finanzfluss.de/anbieter/adac/erfahrungen/"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'de-DE,de;q=0.9',
        })
    
    def scrape(self):
        """Scrape ADAC Erfahrungen."""
        print(f"  Fetching page...", end=" ", flush=True)
        
        try:
            resp = self.session.get(self.base_url, timeout=30)
            resp.raise_for_status()
            
            reviews = self._parse_page(resp.text)
            print(f"{len(reviews)} reviews")
            return reviews
            
        except Exception as e:
            print(f"Error: {e}")
            return []
    
    def _parse_page(self, html: str) -> list:
        """Parse Finanzfluss Seite."""
        soup = BeautifulSoup(html, 'lxml')
        reviews = []
        
        # Methode 1: JSON-LD Structured Data
        scripts = soup.find_all('script', {'type': 'application/ld+json'})
        for script in scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, dict) and 'review' in data:
                    for r in data.get('review', []):
                        if isinstance(r, dict) and r.get('reviewBody'):
                            reviews.append({
                                'text': f"[ADAC Finanzfluss] {r['reviewBody'][:600]}",
                                'rating': r.get('reviewRating', {}).get('ratingValue'),
                                'author': r.get('author', {}).get('name') if isinstance(r.get('author'), dict) else None,
                                'date': r.get('datePublished'),
                                'source': 'finanzfluss.de',
                                'source_url': self.base_url,
                            })
            except:
                continue
        
        if reviews:
            return reviews
        
        # Methode 2: Suche nach Review-Containern
        containers = soup.find_all(['div', 'article'], {'class': re.compile(r'review|erfahrung', re.I)})
        
        for container in containers:
            text_el = container.find(['p', 'div'], {'class': re.compile(r'text|content|body')})
            if text_el:
                text = text_el.get_text(strip=True)
                if len(text) > 30:
                    reviews.append({
                        'text': f"[ADAC Finanzfluss] {text[:600]}",
                        'rating': None,
                        'author': None,
                        'date': None,
                        'source': 'finanzfluss.de',
                        'source_url': self.base_url,
                    })
        
        # Methode 3: Text-Pattern Matching
        if not reviews:
            text = soup.get_text()
            pattern = r'(?:Erfahrung|Bewertung)\s*#?(\d+)'
            for match in re.finditer(pattern, text, re.I):
                start = match.end()
                chunk = text[start:start+500]
                chunk = re.sub(r'\s+', ' ', chunk).strip()
                
                if len(chunk) > 50:
                    reviews.append({
                        'text': f"[ADAC Finanzfluss #{match.group(1)}] {chunk[:400]}",
                        'rating': None,
                        'author': None,
                        'date': None,
                        'source': 'finanzfluss.de',
                        'source_url': self.base_url,
                    })
        
        return reviews


# ============================================================================
# GOOGLE PLAY STORE SCRAPER
# ============================================================================

class GooglePlayScraper:
    """Google Play ADAC App Reviews."""
    
    name = "google_play"
    
    # ADAC Apps im Play Store
    apps = [
        ("ADAC", "de.adac.android.adac"),
        ("ADAC Spritpreise", "de.adac.android.spritpreise"),
        ("ADAC Pannenhilfe", "de.adac.android.pannenhilfe"),
        ("ADAC Camping", "de.adac.android.camping"),
        ("ADAC Maps", "de.adac.android.maps"),
    ]
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'de-DE,de;q=0.9',
        })
    
    def scrape(self):
        """Scrape ADAC App Reviews."""
        all_reviews = []
        
        for app_name, package_id in self.apps:
            url = f"https://play.google.com/store/apps/details?id={package_id}&hl=de&gl=DE"
            print(f"  {app_name}...", end=" ", flush=True)
            
            try:
                resp = self.session.get(url, timeout=30)
                
                if resp.status_code == 404:
                    print("nicht gefunden")
                    continue
                
                resp.raise_for_status()
                reviews = self._parse_page(resp.text, app_name, url)
                all_reviews.extend(reviews)
                print(f"{len(reviews)} reviews")
                
                time.sleep(0.5)
                
            except Exception as e:
                print(f"Error: {e}")
                continue
        
        return all_reviews
    
    def _parse_page(self, html: str, app_name: str, url: str) -> list:
        """Parse Play Store Seite."""
        soup = BeautifulSoup(html, 'lxml')
        reviews = []
        
        # Suche Review-Container
        review_divs = soup.find_all('div', {'data-reviewid': True})
        
        for div in review_divs:
            try:
                # Text
                text_span = div.find('span', {'jsname': True})
                text = text_span.get_text(strip=True) if text_span else ""
                
                if not text or len(text) < 20:
                    continue
                
                # Rating
                rating = None
                rating_div = div.find('div', {'aria-label': re.compile(r'Stern')})
                if rating_div:
                    match = re.search(r'(\d)', rating_div.get('aria-label', ''))
                    if match:
                        rating = float(match.group(1))
                
                reviews.append({
                    'text': f"[{app_name} App] {text[:400]}",
                    'rating': rating,
                    'author': None,
                    'date': None,
                    'source': 'play.google.com',
                    'source_url': url,
                })
                
            except:
                continue
        
        return reviews


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="ADAC Review Scraper V3 - Standalone Edition",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Beispiele:
  python scrape_adac_v3.py                          # Alle Quellen
  python scrape_adac_v3.py --max-pages 50           # Limitiert Trustpilot auf 50 Seiten
  python scrape_adac_v3.py --sources trustpilot     # Nur Trustpilot
  python scrape_adac_v3.py --sources kununu         # Nur Kununu
        """
    )
    parser.add_argument('--max-pages', type=int, default=100,
                       help='Max Seiten für Trustpilot (default: 100 = ~7000 Reviews)')
    parser.add_argument('--output', type=str, default='adac_reviews_v3.json',
                       help='Output Datei')
    parser.add_argument('--sources', nargs='+',
                       choices=['trustpilot', 'finanzfluss', 'kununu', 'playstore', 'all'],
                       default=['all'],
                       help='Quellen (default: all)')
    
    args = parser.parse_args()
    
    print("=" * 70)
    print(" ADAC REVIEW SCRAPER V3 - STANDALONE EDITION")
    print("=" * 70)
    print(f" Output: {args.output}")
    print(f" Max Trustpilot Pages: {args.max_pages}")
    print("=" * 70)
    
    all_reviews = []
    stats = {}
    
    sources = args.sources if 'all' not in args.sources else ['trustpilot', 'finanzfluss', 'kununu', 'playstore']
    
    # === TRUSTPILOT ===
    if 'trustpilot' in sources:
        print("\n" + "=" * 70)
        print(" TRUSTPILOT.DE - ADAC Reviews")
        print("=" * 70)
        scraper = TrustpilotScraper()
        reviews = scraper.scrape(max_pages=args.max_pages)
        all_reviews.extend(reviews)
        stats['trustpilot'] = len(reviews)
    
    # === FINANZFLUSS ===
    if 'finanzfluss' in sources:
        print("\n" + "=" * 70)
        print(" FINANZFLUSS.DE - ADAC Erfahrungen")
        print("=" * 70)
        scraper = FinanzflussScraper()
        reviews = scraper.scrape()
        all_reviews.extend(reviews)
        stats['finanzfluss'] = len(reviews)
    
    # === KUNUNU ===
    if 'kununu' in sources:
        print("\n" + "=" * 70)
        print(" KUNUNU.COM - ADAC Arbeitgeber")
        print("=" * 70)
        scraper = KununuScraper()
        reviews = scraper.scrape()
        all_reviews.extend(reviews)
        stats['kununu'] = len(reviews)
    
    # === GOOGLE PLAY ===
    if 'playstore' in sources:
        print("\n" + "=" * 70)
        print(" GOOGLE PLAY - ADAC Apps")
        print("=" * 70)
        scraper = GooglePlayScraper()
        reviews = scraper.scrape()
        all_reviews.extend(reviews)
        stats['playstore'] = len(reviews)
    
    # === RESULTS ===
    print("\n" + "=" * 70)
    print(" ERGEBNIS")
    print("=" * 70)
    
    if all_reviews:
        # Save full data
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(all_reviews, f, ensure_ascii=False, indent=2)
        
        # Save training format
        training_file = args.output.replace('.json', '_training.json')
        training_data = [{'id': i+1, 'text': r['text']} for i, r in enumerate(all_reviews)]
        with open(training_file, 'w', encoding='utf-8') as f:
            json.dump(training_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n✅ TOTAL: {len(all_reviews)} Reviews gesammelt!")
        print()
        print("Statistik:")
        for source, count in stats.items():
            print(f"  • {source}: {count} reviews")
        print()
        print(f"📁 Full data: {args.output}")
        print(f"📁 Training:  {training_file}")
        print("=" * 70)
        
        # Samples
        print("\nBeispiele:")
        for i, r in enumerate(all_reviews[:5]):
            rating_str = f"★{r['rating']}" if r.get('rating') else "☆"
            print(f"[{i+1}] {rating_str} {r['text'][:70]}...")
    else:
        print("❌ Keine Reviews gesammelt!")


if __name__ == "__main__":
    main()