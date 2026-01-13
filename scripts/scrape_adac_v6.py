"""
ADAC Review Scraper V6 - MIT BROWSER SUPPORT

Das Problem: Trustpilot lädt Reviews per JavaScript nach.
Lösung: Selenium/Playwright für echtes Browser-Rendering.

INSTALLATION:
    pip install requests beautifulsoup4 lxml selenium webdriver-manager

OHNE SELENIUM (nur statische Quellen):
    python scrape_adac_v6.py --no-browser

MIT SELENIUM (Trustpilot funktioniert):
    python scrape_adac_v6.py
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("pip install requests beautifulsoup4 lxml")
    sys.exit(1)

# Optional: Selenium
SELENIUM_AVAILABLE = False
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from webdriver_manager.chrome import ChromeDriverManager
    SELENIUM_AVAILABLE = True
except ImportError:
    pass


# ============================================================================
# TRUSTPILOT MIT SELENIUM
# ============================================================================

class TrustpilotSeleniumScraper:
    """Trustpilot mit echtem Browser - lädt JavaScript-Content."""
    
    name = "trustpilot"
    base_url = "https://de.trustpilot.com/review/www.adac.de"
    
    def __init__(self):
        self.driver = None
    
    def _init_driver(self):
        """Initialisiere Chrome WebDriver."""
        options = Options()
        options.add_argument('--headless')  # Kein sichtbares Fenster
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--lang=de-DE')
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36')
        
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)
        self.driver.implicitly_wait(10)
    
    def scrape(self, max_pages=100):
        """Scrape Trustpilot mit Selenium."""
        if not SELENIUM_AVAILABLE:
            print("  ⚠️  Selenium nicht installiert!")
            print("  Installiere: pip install selenium webdriver-manager")
            return []
        
        print("  Starte Chrome Browser...")
        self._init_driver()
        
        all_reviews = []
        seen = set()
        consecutive_empty = 0
        
        try:
            for page in range(1, max_pages + 1):
                url = f"{self.base_url}?page={page}"
                
                try:
                    self.driver.get(url)
                    time.sleep(2)  # Warte auf JavaScript
                    
                    # Scrolle um lazy-loaded Content zu laden
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
                    time.sleep(1)
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(1)
                    
                    # Parse die gerenderte Seite
                    html = self.driver.page_source
                    reviews = self._parse_page(html, url, seen)
                    
                    if not reviews:
                        consecutive_empty += 1
                        if consecutive_empty >= 3:
                            print(f"  3 leere Seiten - Ende bei Page {page}")
                            break
                        continue
                    
                    consecutive_empty = 0
                    all_reviews.extend(reviews)
                    
                    if page % 10 == 0:
                        print(f"  Page {page}: {len(all_reviews)} total")
                    
                except Exception as e:
                    print(f"  Error Page {page}: {e}")
                    continue
            
        finally:
            if self.driver:
                self.driver.quit()
        
        print(f"  Fertig: {len(all_reviews)} Reviews")
        return all_reviews
    
    def _parse_page(self, html: str, url: str, seen: set) -> list:
        """Parse gerenderte HTML."""
        soup = BeautifulSoup(html, 'lxml')
        reviews = []
        
        # Methode 1: data-service-review-* Attribute
        for rating_el in soup.find_all(attrs={'data-service-review-rating': True}):
            try:
                rating = float(rating_el.get('data-service-review-rating', 0))
                container = rating_el.find_parent(['article', 'section', 'div'])
                
                if not container:
                    continue
                
                title = ""
                title_el = container.find(attrs={'data-service-review-title-typography': True})
                if title_el:
                    title = title_el.get_text(strip=True)
                
                text = ""
                text_el = container.find(attrs={'data-service-review-text-typography': True})
                if text_el:
                    text = text_el.get_text(strip=True)
                
                if not text and not title:
                    continue
                
                full_text = f"{title}\n{text}".strip() if title else text
                
                if len(full_text) < 20 or full_text[:50] in seen:
                    continue
                seen.add(full_text[:50])
                
                author = None
                author_el = container.find(attrs={'data-consumer-name-typography': True})
                if author_el:
                    author = author_el.get_text(strip=True)
                
                reviews.append({
                    'text': f"[ADAC Trustpilot] {full_text}",
                    'rating': rating,
                    'author': author,
                    'date': None,
                    'source': 'trustpilot.de',
                    'source_url': url,
                })
            except:
                continue
        
        return reviews


# ============================================================================
# TRUSTPILOT OHNE SELENIUM (Fallback)
# ============================================================================

class TrustpilotStaticScraper:
    """Trustpilot ohne Browser - limitiert aber funktioniert."""
    
    name = "trustpilot_static"
    base_url = "https://de.trustpilot.com/review/www.adac.de"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
        })
    
    def scrape(self, max_pages=30):
        """Scrape was ohne JavaScript möglich ist."""
        all_reviews = []
        seen = set()
        
        for page in range(1, max_pages + 1):
            url = f"{self.base_url}?page={page}"
            
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                
                # Versuche __NEXT_DATA__ zu parsen
                soup = BeautifulSoup(resp.text, 'lxml')
                next_data = soup.find('script', {'id': '__NEXT_DATA__'})
                
                if next_data:
                    try:
                        data = json.loads(next_data.string)
                        reviews_data = data.get('props', {}).get('pageProps', {}).get('reviews', [])
                        
                        for r in reviews_data:
                            text = r.get('text', '')
                            title = r.get('title', '')
                            full_text = f"{title}\n{text}".strip() if title else text
                            
                            if len(full_text) < 20 or full_text[:50] in seen:
                                continue
                            seen.add(full_text[:50])
                            
                            all_reviews.append({
                                'text': f"[ADAC Trustpilot] {full_text}",
                                'rating': r.get('rating'),
                                'author': r.get('consumer', {}).get('displayName'),
                                'date': r.get('dates', {}).get('publishedDate', '')[:10] if r.get('dates') else None,
                                'source': 'trustpilot.de',
                                'source_url': url,
                            })
                    except:
                        pass
                
                if page % 10 == 0:
                    print(f"  Page {page}: {len(all_reviews)} total")
                
                time.sleep(0.8)
                
            except Exception as e:
                if "404" in str(e):
                    break
                continue
        
        print(f"  Fertig: {len(all_reviews)} Reviews (Static)")
        return all_reviews


# ============================================================================
# KUNUNU SCRAPER - VERBESSERT
# ============================================================================

class KununuScraper:
    """Kununu ADAC Arbeitgeber-Bewertungen."""
    
    name = "kununu"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'de-DE,de;q=0.9',
        })
    
    def scrape(self, max_pages=100):
        """Scrape Kununu Reviews."""
        all_reviews = []
        seen = set()
        
        base_url = "https://www.kununu.com/de/adac/kommentare"
        
        for page in range(1, max_pages + 1):
            url = f"{base_url}?page={page}" if page > 1 else base_url
            
            try:
                resp = self.session.get(url, timeout=30)
                
                if resp.status_code == 404:
                    break
                
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, 'lxml')
                
                # Finde Review-Blöcke
                review_blocks = soup.find_all(['article', 'div'], {'class': re.compile(r'index__reviewBlock', re.I)})
                
                if not review_blocks:
                    # Alternative Selektoren
                    review_blocks = soup.find_all('article')
                
                if not review_blocks:
                    break
                
                page_count = 0
                for block in review_blocks:
                    # Sammle Text
                    texts = []
                    
                    # Titel
                    title_el = block.find(['h2', 'h3'])
                    if title_el:
                        texts.append(title_el.get_text(strip=True))
                    
                    # Pro/Contra
                    for section in ['Gut am Arbeitgeber', 'Schlecht am Arbeitgeber', 'Verbesserungsvorschläge']:
                        header = block.find(string=re.compile(section, re.I))
                        if header:
                            next_p = header.find_next(['p', 'div', 'span'])
                            if next_p:
                                text = next_p.get_text(strip=True)
                                if len(text) > 10:
                                    texts.append(f"{section}: {text}")
                    
                    # Alle Paragraphen als Fallback
                    if not texts:
                        for p in block.find_all('p'):
                            p_text = p.get_text(strip=True)
                            if len(p_text) > 30:
                                texts.append(p_text)
                    
                    if not texts:
                        continue
                    
                    full_text = ' | '.join(texts[:4])
                    
                    if len(full_text) < 30 or full_text[:50] in seen:
                        continue
                    seen.add(full_text[:50])
                    
                    # Rating
                    rating = None
                    score_el = block.find(['span', 'div'], string=re.compile(r'^\d[,.]?\d$'))
                    if score_el:
                        try:
                            rating = float(score_el.get_text().replace(',', '.'))
                        except:
                            pass
                    
                    all_reviews.append({
                        'text': f"[ADAC Kununu] {full_text[:600]}",
                        'rating': rating,
                        'author': None,
                        'date': None,
                        'source': 'kununu.com',
                        'source_url': url,
                    })
                    page_count += 1
                
                if page_count == 0:
                    break
                
                if page % 20 == 0:
                    print(f"  Page {page}: {len(all_reviews)} total")
                
                time.sleep(0.5)
                
            except Exception as e:
                continue
        
        print(f"  Fertig: {len(all_reviews)} Reviews")
        return all_reviews


# ============================================================================
# PROVENEXPERT SCRAPER - NEU!
# ============================================================================

class ProvenExpertScraper:
    """ProvenExpert ADAC Bewertungen - oft übersehene Quelle!"""
    
    name = "provenexpert"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'de-DE,de;q=0.9',
        })
    
    def scrape(self):
        """Scrape ProvenExpert."""
        all_reviews = []
        
        # Suche nach ADAC auf ProvenExpert
        search_url = "https://www.provenexpert.com/de-de/suche/?q=ADAC"
        
        try:
            resp = self.session.get(search_url, timeout=30)
            resp.raise_for_status()
            
            soup = BeautifulSoup(resp.text, 'lxml')
            
            # Finde Profile-Links
            profile_links = soup.find_all('a', href=re.compile(r'/[a-z0-9-]+/$'))
            
            for link in profile_links[:10]:
                href = link.get('href', '')
                if 'adac' in href.lower():
                    profile_url = f"https://www.provenexpert.com{href}"
                    reviews = self._scrape_profile(profile_url)
                    all_reviews.extend(reviews)
            
        except Exception as e:
            print(f"  Error: {e}")
        
        print(f"  Fertig: {len(all_reviews)} Reviews")
        return all_reviews
    
    def _scrape_profile(self, url: str) -> list:
        """Scrape ein ProvenExpert Profil."""
        reviews = []
        
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            
            soup = BeautifulSoup(resp.text, 'lxml')
            
            # Finde Review-Container
            for container in soup.find_all(['div', 'article'], {'class': re.compile(r'review|rating', re.I)}):
                text = container.get_text(strip=True)
                if len(text) > 50:
                    reviews.append({
                        'text': f"[ADAC ProvenExpert] {text[:500]}",
                        'rating': None,
                        'author': None,
                        'date': None,
                        'source': 'provenexpert.com',
                        'source_url': url,
                    })
            
            time.sleep(0.5)
            
        except:
            pass
        
        return reviews


# ============================================================================
# AUSGEZEICHNET.ORG SCRAPER - NEU!
# ============================================================================

class AusgezeichnetScraper:
    """Ausgezeichnet.org Bewertungen."""
    
    name = "ausgezeichnet"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'de-DE,de;q=0.9',
        })
    
    def scrape(self):
        """Scrape ausgezeichnet.org."""
        all_reviews = []
        
        # ADAC Profile auf ausgezeichnet.org
        urls = [
            "https://www.ausgezeichnet.org/bewertungen-adac",
            "https://www.ausgezeichnet.org/bewertungen-adac-de",
        ]
        
        for url in urls:
            try:
                resp = self.session.get(url, timeout=30)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, 'lxml')
                    
                    for container in soup.find_all(['div', 'article'], {'class': re.compile(r'review|bewertung', re.I)}):
                        text = container.get_text(strip=True)
                        if len(text) > 50:
                            all_reviews.append({
                                'text': f"[ADAC Ausgezeichnet.org] {text[:500]}",
                                'rating': None,
                                'author': None,
                                'date': None,
                                'source': 'ausgezeichnet.org',
                                'source_url': url,
                            })
                    
                time.sleep(0.5)
            except:
                pass
        
        print(f"  Fertig: {len(all_reviews)} Reviews")
        return all_reviews


# ============================================================================
# GOOGLE PLAY STORE - VERBESSERT
# ============================================================================

class GooglePlayScraper:
    """Google Play Reviews via Data Safety Seite."""
    
    name = "google_play"
    
    APPS = [
        ("ADAC", "de.adac.android.adac"),
        ("ADAC Spritpreise", "de.adac.android.spritpreise"),
        ("ADAC Pannenhilfe", "de.adac.android.pannenhilfe"),
        ("ADAC Maps", "de.adac.android.maps"),
        ("ADAC Camping", "de.adac.android.camping"),
        ("ADAC Trips", "de.adac.android.trips"),
    ]
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'de-DE,de;q=0.9',
        })
    
    def scrape(self):
        """Scrape Google Play Reviews."""
        all_reviews = []
        
        for app_name, package_id in self.APPS:
            print(f"  {app_name}...", end=" ", flush=True)
            
            url = f"https://play.google.com/store/apps/details?id={package_id}&hl=de&gl=DE&showAllReviews=true"
            
            try:
                resp = self.session.get(url, timeout=30)
                
                if resp.status_code == 404:
                    print("nicht gefunden")
                    continue
                
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, 'lxml')
                
                # Suche Review-Elemente
                review_count = 0
                
                # Methode 1: Review-Divs mit jscontroller
                for div in soup.find_all('div', {'jscontroller': True}):
                    text = div.get_text(strip=True)
                    if len(text) > 50 and len(text) < 1000:
                        # Prüfe ob es ein Review ist (hat Rating-Indikator)
                        if div.find(['span', 'div'], {'aria-label': re.compile(r'Stern', re.I)}):
                            all_reviews.append({
                                'text': f"[{app_name} App] {text[:400]}",
                                'rating': None,
                                'author': None,
                                'date': None,
                                'source': 'play.google.com',
                                'source_url': url,
                            })
                            review_count += 1
                
                print(f"{review_count} reviews")
                time.sleep(0.5)
                
            except Exception as e:
                print(f"Error: {e}")
        
        print(f"  Fertig: {len(all_reviews)} Reviews")
        return all_reviews


# ============================================================================
# APPLE APP STORE - RSS API
# ============================================================================

class AppStoreScraper:
    """Apple App Store Reviews via RSS API."""
    
    name = "appstore"
    
    APPS = [
        ("ADAC", "410683848"),
        ("ADAC Spritpreise", "450498498"),
        ("ADAC Camping", "498457072"),
        ("ADAC Maps", "921967184"),
        ("ADAC Pannenhilfe", "1437578071"),
    ]
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
            'Accept': 'application/json',
        })
    
    def scrape(self):
        """Scrape App Store Reviews."""
        all_reviews = []
        
        for app_name, app_id in self.APPS:
            print(f"  {app_name}...", end=" ", flush=True)
            
            # Apple RSS API
            url = f"https://itunes.apple.com/de/rss/customerreviews/id={app_id}/sortBy=mostRecent/json"
            
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                
                data = resp.json()
                entries = data.get('feed', {}).get('entry', [])
                
                # Erste Entry ist oft die App-Info, nicht ein Review
                if entries and 'im:rating' not in entries[0]:
                    entries = entries[1:]
                
                count = 0
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    
                    content = entry.get('content', {})
                    if isinstance(content, dict):
                        text = content.get('label', '')
                    else:
                        continue
                    
                    title = entry.get('title', {}).get('label', '')
                    rating = entry.get('im:rating', {}).get('label')
                    author = entry.get('author', {}).get('name', {}).get('label')
                    
                    if text and len(text) > 20:
                        full_text = f"{title}\n{text}" if title else text
                        
                        all_reviews.append({
                            'text': f"[{app_name} iOS] {full_text[:400]}",
                            'rating': float(rating) if rating else None,
                            'author': author,
                            'date': None,
                            'source': 'apps.apple.com',
                            'source_url': f"https://apps.apple.com/de/app/id{app_id}",
                        })
                        count += 1
                
                print(f"{count} reviews")
                time.sleep(0.3)
                
            except Exception as e:
                print(f"Error: {e}")
        
        print(f"  Fertig: {len(all_reviews)} Reviews")
        return all_reviews


# ============================================================================
# EKOMI SCRAPER - NEU!
# ============================================================================

class EkomiScraper:
    """eKomi Bewertungen - beliebte deutsche Bewertungsplattform."""
    
    name = "ekomi"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'de-DE,de;q=0.9',
        })
    
    def scrape(self):
        """Scrape eKomi."""
        all_reviews = []
        
        # eKomi ADAC Suche
        search_url = "https://www.ekomi.de/bewertungen-adac.html"
        
        try:
            resp = self.session.get(search_url, timeout=30)
            
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'lxml')
                
                for container in soup.find_all(['div', 'article'], {'class': re.compile(r'review|feedback', re.I)}):
                    text = container.get_text(strip=True)
                    if len(text) > 30:
                        all_reviews.append({
                            'text': f"[ADAC eKomi] {text[:500]}",
                            'rating': None,
                            'author': None,
                            'date': None,
                            'source': 'ekomi.de',
                            'source_url': search_url,
                        })
            
        except Exception as e:
            print(f"  Error: {e}")
        
        print(f"  Fertig: {len(all_reviews)} Reviews")
        return all_reviews


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="ADAC Review Scraper V6")
    parser.add_argument('--max-pages', type=int, default=100, help='Max Trustpilot Pages')
    parser.add_argument('--output', type=str, default='adac_reviews_v6.json', help='Output file')
    parser.add_argument('--no-browser', action='store_true', help='Kein Selenium verwenden')
    
    args = parser.parse_args()
    
    print("=" * 70)
    print(" ADAC REVIEW SCRAPER V6")
    print("=" * 70)
    
    if SELENIUM_AVAILABLE and not args.no_browser:
        print(" ✓ Selenium verfügbar - Trustpilot mit Browser")
    else:
        print(" ⚠ Selenium nicht verfügbar - limitierte Trustpilot-Daten")
        print("   Installiere: pip install selenium webdriver-manager")
    
    print(f" Output: {args.output}")
    print("=" * 70)
    
    all_reviews = []
    stats = {}
    
    # === TRUSTPILOT ===
    print(f"\n{'='*70}")
    print(" TRUSTPILOT")
    print("=" * 70)
    
    if SELENIUM_AVAILABLE and not args.no_browser:
        scraper = TrustpilotSeleniumScraper()
        reviews = scraper.scrape(max_pages=args.max_pages)
    else:
        scraper = TrustpilotStaticScraper()
        reviews = scraper.scrape(max_pages=30)
    
    all_reviews.extend(reviews)
    stats['trustpilot'] = len(reviews)
    
    # === KUNUNU ===
    print(f"\n{'='*70}")
    print(" KUNUNU")
    print("=" * 70)
    scraper = KununuScraper()
    reviews = scraper.scrape(max_pages=100)
    all_reviews.extend(reviews)
    stats['kununu'] = len(reviews)
    
    # === APP STORE ===
    print(f"\n{'='*70}")
    print(" APPLE APP STORE")
    print("=" * 70)
    scraper = AppStoreScraper()
    reviews = scraper.scrape()
    all_reviews.extend(reviews)
    stats['appstore'] = len(reviews)
    
    # === GOOGLE PLAY ===
    print(f"\n{'='*70}")
    print(" GOOGLE PLAY")
    print("=" * 70)
    scraper = GooglePlayScraper()
    reviews = scraper.scrape()
    all_reviews.extend(reviews)
    stats['playstore'] = len(reviews)
    
    # === PROVENEXPERT ===
    print(f"\n{'='*70}")
    print(" PROVENEXPERT")
    print("=" * 70)
    scraper = ProvenExpertScraper()
    reviews = scraper.scrape()
    all_reviews.extend(reviews)
    stats['provenexpert'] = len(reviews)
    
    # === EKOMI ===
    print(f"\n{'='*70}")
    print(" EKOMI")
    print("=" * 70)
    scraper = EkomiScraper()
    reviews = scraper.scrape()
    all_reviews.extend(reviews)
    stats['ekomi'] = len(reviews)
    
    # === ERGEBNIS ===
    print(f"\n{'='*70}")
    print(" ERGEBNIS")
    print("=" * 70)
    
    if all_reviews:
        # Deduplizierung
        seen = set()
        unique = []
        for r in all_reviews:
            key = r['text'][:80]
            if key not in seen:
                seen.add(key)
                unique.append(r)
        
        # Save
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(unique, f, ensure_ascii=False, indent=2)
        
        training_file = args.output.replace('.json', '_training.json')
        training_data = [{'id': i+1, 'text': r['text']} for i, r in enumerate(unique)]
        with open(training_file, 'w', encoding='utf-8') as f:
            json.dump(training_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n  Roh: {len(all_reviews)}")
        print(f"  Unique: {len(unique)}")
        print("\n  Statistik:")
        for source, count in stats.items():
            print(f"    • {source}: {count}")
        print(f"\n  ✅ TOTAL: {len(unique)} Reviews!")
        print(f"  📁 {args.output}")
        print(f"  📁 {training_file}")
    else:
        print("  ❌ Keine Reviews!")


if __name__ == "__main__":
    main()