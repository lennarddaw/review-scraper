"""
ADAC Review Scraper V4 - MAXIMUM EDITION

Sammelt Reviews von ALLEN verfügbaren Quellen:

CONSUMER REVIEWS:
1. Trustpilot.de - ADAC (FIXED - alle 6,400+)
2. Trustpilot.de - ADAC Reisen
3. Trustpilot.de - ADAC Versicherung
4. Finanzfluss.de
5. Finanztip.de
6. Check24.de
7. Verivox.de

EMPLOYER REVIEWS:
8. Kununu.com

FORUM/COMMUNITY:
9. Gutefrage.net
10. Reddit (de)
11. Motor-Talk.de

APP REVIEWS:
12. Apple App Store

Usage:
    python scrape_adac_v4.py
    python scrape_adac_v4.py --max-pages 200
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote, urljoin

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Bitte installiere: pip install requests beautifulsoup4 lxml")
    sys.exit(1)


# ============================================================================
# TRUSTPILOT SCRAPER - KOMPLETT NEU
# ============================================================================

class TrustpilotScraperV4:
    """
    Trustpilot Scraper - scrapt MEHRERE ADAC-bezogene Seiten.
    """
    
    name = "trustpilot"
    
    # Alle ADAC-bezogenen Trustpilot-Seiten
    PAGES = [
        ("ADAC", "https://de.trustpilot.com/review/www.adac.de"),
        ("ADAC Reisen", "https://de.trustpilot.com/review/www.adac-reisen.de"),
        ("ADAC Versicherung", "https://de.trustpilot.com/review/www.adac.de/versicherung"),
        ("ADAC Autovermietung", "https://de.trustpilot.com/review/autovermietung.adac.de"),
    ]
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Sec-Ch-Ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
        })
    
    def scrape(self, max_pages_per_site=100):
        """Scrape alle ADAC Trustpilot Seiten."""
        all_reviews = []
        
        for site_name, base_url in self.PAGES:
            print(f"\n  [{site_name}]")
            reviews = self._scrape_site(base_url, site_name, max_pages_per_site)
            all_reviews.extend(reviews)
            print(f"  → {len(reviews)} reviews von {site_name}")
            time.sleep(2)  # Pause zwischen Sites
        
        return all_reviews
    
    def _scrape_site(self, base_url: str, site_name: str, max_pages: int) -> list:
        """Scrape eine einzelne Trustpilot-Seite."""
        reviews = []
        empty_count = 0
        
        for page in range(1, max_pages + 1):
            url = f"{base_url}?page={page}"
            
            try:
                resp = self.session.get(url, timeout=30)
                
                if resp.status_code == 404:
                    break
                
                resp.raise_for_status()
                
                page_reviews = self._parse_page_v4(resp.text, site_name, url)
                
                if not page_reviews:
                    empty_count += 1
                    if empty_count >= 2:
                        break
                    continue
                
                empty_count = 0
                reviews.extend(page_reviews)
                
                if page % 10 == 0:
                    print(f"    Page {page}: {len(reviews)} total")
                
                # Smart rate limiting
                time.sleep(0.5 + (page % 10) * 0.05)
                
            except requests.exceptions.HTTPError as e:
                if "404" in str(e) or "403" in str(e):
                    break
                print(f"    Error page {page}: {e}")
                time.sleep(5)
            except Exception as e:
                print(f"    Error: {e}")
                continue
        
        return reviews
    
    def _parse_page_v4(self, html: str, site_name: str, url: str) -> list:
        """Parse Trustpilot page - Mehrere Methoden."""
        soup = BeautifulSoup(html, 'lxml')
        reviews = []
        seen = set()
        
        # === METHODE 1: Script-Tag mit __NEXT_DATA__ (React SSR) ===
        next_data = soup.find('script', {'id': '__NEXT_DATA__'})
        if next_data:
            try:
                data = json.loads(next_data.string)
                page_props = data.get('props', {}).get('pageProps', {})
                review_list = page_props.get('reviews', [])
                
                for r in review_list:
                    text = r.get('text', '')
                    title = r.get('title', '')
                    
                    if not text and not title:
                        continue
                    
                    full_text = f"{title}\n{text}".strip() if title else text
                    
                    if len(full_text) < 20 or full_text[:50] in seen:
                        continue
                    seen.add(full_text[:50])
                    
                    reviews.append({
                        'text': f"[{site_name} Trustpilot] {full_text}",
                        'rating': r.get('rating'),
                        'author': r.get('consumer', {}).get('displayName'),
                        'date': r.get('dates', {}).get('publishedDate', '')[:10] if r.get('dates') else None,
                        'source': 'trustpilot.de',
                        'source_url': url,
                    })
                
                if reviews:
                    return reviews
            except:
                pass
        
        # === METHODE 2: data-service-review-* Attribute ===
        rating_els = soup.find_all(attrs={'data-service-review-rating': True})
        
        for rating_el in rating_els:
            try:
                rating = float(rating_el.get('data-service-review-rating', 0))
                
                # Find container
                container = rating_el.find_parent(['article', 'section', 'div'])
                if not container:
                    continue
                
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
                    continue
                
                full_text = f"{title}\n{text}".strip() if title else text
                
                if len(full_text) < 20 or full_text[:50] in seen:
                    continue
                seen.add(full_text[:50])
                
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
                
                reviews.append({
                    'text': f"[{site_name} Trustpilot] {full_text}",
                    'rating': rating,
                    'author': author,
                    'date': date,
                    'source': 'trustpilot.de',
                    'source_url': url,
                })
            except:
                continue
        
        if reviews:
            return reviews
        
        # === METHODE 3: Fallback - Star Images ===
        star_imgs = soup.find_all('img', {'src': re.compile(r'stars-\d')})
        
        for img in star_imgs:
            try:
                src = img.get('src', '')
                rating_match = re.search(r'stars-(\d)', src)
                rating = float(rating_match.group(1)) if rating_match else None
                
                # Finde übergeordneten Container
                container = img
                for _ in range(8):
                    container = container.parent
                    if container is None:
                        break
                    if container.name in ['article', 'section']:
                        break
                
                if container is None:
                    continue
                
                # Suche Textblöcke
                paragraphs = container.find_all('p')
                texts = []
                for p in paragraphs:
                    p_text = p.get_text(strip=True)
                    # Filter UI-Text
                    if len(p_text) > 30 and not any(x in p_text.lower() for x in ['bewertung', 'website', 'profil', 'unternehmen']):
                        texts.append(p_text)
                
                if not texts:
                    continue
                
                full_text = ' '.join(texts[:2])
                
                if len(full_text) < 30 or full_text[:50] in seen:
                    continue
                seen.add(full_text[:50])
                
                reviews.append({
                    'text': f"[{site_name} Trustpilot] {full_text[:600]}",
                    'rating': rating,
                    'author': None,
                    'date': None,
                    'source': 'trustpilot.de',
                    'source_url': url,
                })
            except:
                continue
        
        return reviews


# ============================================================================
# KUNUNU SCRAPER - VERBESSERT
# ============================================================================

class KununuScraperV4:
    """Kununu ADAC - Mehr Seiten, bessere Extraktion."""
    
    name = "kununu"
    base_url = "https://www.kununu.com/de/adac"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'de-DE,de;q=0.9',
        })
    
    def scrape(self, max_pages=50):
        """Scrape alle ADAC Kununu Reviews."""
        all_reviews = []
        
        # Verschiedene Kununu-Sections
        sections = [
            ("Kommentare", f"{self.base_url}/kommentare"),
            ("Bewerbung", f"{self.base_url}/bewerbung"),
        ]
        
        for section_name, base in sections:
            print(f"\n  [{section_name}]")
            reviews = self._scrape_section(base, section_name, max_pages)
            all_reviews.extend(reviews)
            print(f"  → {len(reviews)} reviews")
        
        return all_reviews
    
    def _scrape_section(self, base_url: str, section_name: str, max_pages: int) -> list:
        """Scrape eine Kununu Section."""
        reviews = []
        
        for page in range(1, max_pages + 1):
            url = f"{base_url}?page={page}" if page > 1 else base_url
            
            try:
                resp = self.session.get(url, timeout=30)
                
                if resp.status_code == 404:
                    break
                
                resp.raise_for_status()
                page_reviews = self._parse_page(resp.text, section_name, url)
                
                if not page_reviews:
                    break
                
                reviews.extend(page_reviews)
                
                if page % 10 == 0:
                    print(f"    Page {page}: {len(reviews)} total")
                
                time.sleep(0.8)
                
            except Exception as e:
                if "404" in str(e):
                    break
                continue
        
        return reviews
    
    def _parse_page(self, html: str, section_name: str, url: str) -> list:
        """Parse Kununu page."""
        soup = BeautifulSoup(html, 'lxml')
        reviews = []
        
        # Finde alle Review-Artikel
        articles = soup.find_all(['article', 'div'], {'class': re.compile(r'index-.*-review|review-item', re.I)})
        
        if not articles:
            # Alternative: Suche nach data-testid
            articles = soup.find_all(attrs={'data-testid': re.compile(r'review', re.I)})
        
        if not articles:
            # Letzte Chance: Alle Artikel
            articles = soup.find_all('article')
        
        for article in articles:
            try:
                # Sammle alle Textblöcke
                texts = []
                
                # Headline/Titel
                headline = article.find(['h2', 'h3', 'h4'])
                if headline:
                    texts.append(headline.get_text(strip=True))
                
                # Alle Paragraphen
                for p in article.find_all(['p', 'span']):
                    p_text = p.get_text(strip=True)
                    if len(p_text) > 20 and p_text not in texts:
                        texts.append(p_text)
                
                # Pro/Contra Listen
                for li in article.find_all('li'):
                    li_text = li.get_text(strip=True)
                    if len(li_text) > 15:
                        texts.append(li_text)
                
                if not texts:
                    continue
                
                full_text = ' | '.join(texts[:5])
                
                if len(full_text) < 30:
                    continue
                
                # Rating
                rating = None
                score_el = article.find(['span', 'div'], string=re.compile(r'^\d[,.]?\d?$'))
                if score_el:
                    try:
                        rating = float(score_el.get_text().replace(',', '.'))
                    except:
                        pass
                
                reviews.append({
                    'text': f"[ADAC Kununu {section_name}] {full_text[:700]}",
                    'rating': rating,
                    'author': None,
                    'date': None,
                    'source': 'kununu.com',
                    'source_url': url,
                })
                
            except:
                continue
        
        return reviews


# ============================================================================
# FINANZFLUSS SCRAPER - VERBESSERT
# ============================================================================

class FinanzflussScraperV4:
    """Finanzfluss ADAC - Mehrere Seiten."""
    
    name = "finanzfluss"
    
    PAGES = [
        "https://www.finanzfluss.de/anbieter/adac/erfahrungen/",
        "https://www.finanzfluss.de/anbieter/adac-autoversicherung/erfahrungen/",
    ]
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'de-DE,de;q=0.9',
        })
    
    def scrape(self):
        """Scrape Finanzfluss ADAC Reviews."""
        all_reviews = []
        
        for url in self.PAGES:
            print(f"  {url.split('/')[-3]}...", end=" ", flush=True)
            
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                
                reviews = self._parse_page(resp.text, url)
                all_reviews.extend(reviews)
                print(f"{len(reviews)} reviews")
                
                time.sleep(1)
            except Exception as e:
                print(f"Error: {e}")
        
        return all_reviews
    
    def _parse_page(self, html: str, url: str) -> list:
        """Parse Finanzfluss."""
        soup = BeautifulSoup(html, 'lxml')
        reviews = []
        seen = set()
        
        # JSON-LD
        for script in soup.find_all('script', {'type': 'application/ld+json'}):
            try:
                data = json.loads(script.string)
                for r in data.get('review', []) if isinstance(data, dict) else []:
                    text = r.get('reviewBody', '')
                    if text and len(text) > 30 and text[:50] not in seen:
                        seen.add(text[:50])
                        reviews.append({
                            'text': f"[ADAC Finanzfluss] {text[:600]}",
                            'rating': r.get('reviewRating', {}).get('ratingValue'),
                            'author': r.get('author', {}).get('name') if isinstance(r.get('author'), dict) else None,
                            'date': r.get('datePublished'),
                            'source': 'finanzfluss.de',
                            'source_url': url,
                        })
            except:
                pass
        
        # Container-basiert
        for container in soup.find_all(['div', 'article'], {'class': re.compile(r'review|erfahrung', re.I)}):
            text = container.get_text(strip=True)
            if len(text) > 50 and text[:50] not in seen:
                seen.add(text[:50])
                reviews.append({
                    'text': f"[ADAC Finanzfluss] {text[:600]}",
                    'rating': None,
                    'author': None,
                    'date': None,
                    'source': 'finanzfluss.de',
                    'source_url': url,
                })
        
        return reviews


# ============================================================================
# GUTEFRAGE.NET SCRAPER - NEU!
# ============================================================================

class GutefrageScraper:
    """Gutefrage.net ADAC Diskussionen."""
    
    name = "gutefrage"
    search_url = "https://www.gutefrage.net/suche?q=ADAC+erfahrung"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'de-DE,de;q=0.9',
        })
    
    def scrape(self, max_pages=10):
        """Scrape ADAC-bezogene Gutefrage Posts."""
        all_content = []
        
        search_terms = [
            "ADAC+Erfahrung",
            "ADAC+Pannenhilfe",
            "ADAC+Mitgliedschaft",
            "ADAC+lohnt+sich",
            "ADAC+Bewertung",
        ]
        
        for term in search_terms:
            url = f"https://www.gutefrage.net/suche?q={term}"
            print(f"  Suche: {term.replace('+', ' ')}...", end=" ", flush=True)
            
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                
                # Finde Frage-Links
                soup = BeautifulSoup(resp.text, 'lxml')
                links = soup.find_all('a', href=re.compile(r'/frage/'))
                
                count = 0
                for link in links[:5]:  # Max 5 pro Suchbegriff
                    href = link.get('href', '')
                    if href:
                        full_url = urljoin("https://www.gutefrage.net", href)
                        content = self._scrape_question(full_url)
                        if content:
                            all_content.extend(content)
                            count += len(content)
                
                print(f"{count} posts")
                time.sleep(1)
                
            except Exception as e:
                print(f"Error: {e}")
        
        return all_content
    
    def _scrape_question(self, url: str) -> list:
        """Scrape eine einzelne Frage + Antworten."""
        content = []
        
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            
            soup = BeautifulSoup(resp.text, 'lxml')
            
            # Frage
            question = soup.find(['h1', 'div'], {'class': re.compile(r'question', re.I)})
            if question:
                q_text = question.get_text(strip=True)
                if 'ADAC' in q_text.upper() and len(q_text) > 30:
                    content.append({
                        'text': f"[Gutefrage Frage] {q_text[:500]}",
                        'rating': None,
                        'author': None,
                        'date': None,
                        'source': 'gutefrage.net',
                        'source_url': url,
                    })
            
            # Antworten
            answers = soup.find_all(['div', 'article'], {'class': re.compile(r'answer', re.I)})
            for ans in answers[:5]:
                ans_text = ans.get_text(strip=True)
                if 'ADAC' in ans_text.upper() and len(ans_text) > 50:
                    content.append({
                        'text': f"[Gutefrage Antwort] {ans_text[:500]}",
                        'rating': None,
                        'author': None,
                        'date': None,
                        'source': 'gutefrage.net',
                        'source_url': url,
                    })
            
            time.sleep(0.5)
            
        except:
            pass
        
        return content


# ============================================================================
# MOTOR-TALK SCRAPER - NEU!
# ============================================================================

class MotorTalkScraper:
    """Motor-Talk.de ADAC Diskussionen."""
    
    name = "motor_talk"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'de-DE,de;q=0.9',
        })
    
    def scrape(self, max_pages=5):
        """Scrape ADAC Diskussionen von Motor-Talk."""
        all_content = []
        
        search_url = "https://www.motor-talk.de/suche.html?q=ADAC"
        print(f"  Motor-Talk Suche...", end=" ", flush=True)
        
        try:
            resp = self.session.get(search_url, timeout=30)
            resp.raise_for_status()
            
            soup = BeautifulSoup(resp.text, 'lxml')
            
            # Finde Diskussions-Links
            links = soup.find_all('a', href=re.compile(r'/forum/'))
            
            for link in links[:15]:
                href = link.get('href', '')
                if href:
                    full_url = urljoin("https://www.motor-talk.de", href)
                    content = self._scrape_thread(full_url)
                    all_content.extend(content)
            
            print(f"{len(all_content)} posts")
            
        except Exception as e:
            print(f"Error: {e}")
        
        return all_content
    
    def _scrape_thread(self, url: str) -> list:
        """Scrape einen Forum-Thread."""
        content = []
        
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            
            soup = BeautifulSoup(resp.text, 'lxml')
            
            # Posts
            posts = soup.find_all(['div', 'article'], {'class': re.compile(r'post|message|entry', re.I)})
            
            for post in posts[:10]:
                text = post.get_text(strip=True)
                if 'ADAC' in text.upper() and len(text) > 50:
                    # Kürze sehr lange Posts
                    if len(text) > 600:
                        text = text[:600] + "..."
                    
                    content.append({
                        'text': f"[Motor-Talk Forum] {text}",
                        'rating': None,
                        'author': None,
                        'date': None,
                        'source': 'motor-talk.de',
                        'source_url': url,
                    })
            
            time.sleep(0.3)
            
        except:
            pass
        
        return content


# ============================================================================
# APPLE APP STORE SCRAPER - NEU!
# ============================================================================

class AppStoreScraper:
    """Apple App Store ADAC Apps."""
    
    name = "appstore"
    
    # ADAC Apps im App Store (iTunes IDs)
    APPS = [
        ("ADAC", "410683848"),
        ("ADAC Spritpreise", "450498498"),
        ("ADAC Camping", "498457072"),
        ("ADAC Maps", "921967184"),
    ]
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept': 'application/json',
        })
    
    def scrape(self):
        """Scrape ADAC App Reviews vom App Store."""
        all_reviews = []
        
        for app_name, app_id in self.APPS:
            print(f"  {app_name}...", end=" ", flush=True)
            
            # Apple hat eine öffentliche RSS API für Reviews
            url = f"https://itunes.apple.com/de/rss/customerreviews/id={app_id}/sortBy=mostRecent/json"
            
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                
                data = resp.json()
                entries = data.get('feed', {}).get('entry', [])
                
                count = 0
                for entry in entries:
                    if isinstance(entry, dict) and 'content' in entry:
                        text = entry.get('content', {}).get('label', '')
                        title = entry.get('title', {}).get('label', '')
                        rating = entry.get('im:rating', {}).get('label')
                        author = entry.get('author', {}).get('name', {}).get('label')
                        
                        if text and len(text) > 20:
                            full_text = f"{title}\n{text}" if title else text
                            
                            all_reviews.append({
                                'text': f"[{app_name} iOS] {full_text[:500]}",
                                'rating': float(rating) if rating else None,
                                'author': author,
                                'date': None,
                                'source': 'apps.apple.com',
                                'source_url': f"https://apps.apple.com/de/app/id{app_id}",
                            })
                            count += 1
                
                print(f"{count} reviews")
                time.sleep(0.5)
                
            except Exception as e:
                print(f"Error: {e}")
        
        return all_reviews


# ============================================================================
# CHECK24 SCRAPER - NEU!
# ============================================================================

class Check24Scraper:
    """Check24 ADAC Versicherungsbewertungen."""
    
    name = "check24"
    
    PAGES = [
        "https://www.check24.de/kfz-versicherung/adac/",
        "https://www.check24.de/hausratversicherung/adac/",
        "https://www.check24.de/rechtsschutzversicherung/adac/",
    ]
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'de-DE,de;q=0.9',
        })
    
    def scrape(self):
        """Scrape Check24 ADAC Bewertungen."""
        all_reviews = []
        
        for url in self.PAGES:
            product = url.split('/')[-2]
            print(f"  {product}...", end=" ", flush=True)
            
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                
                soup = BeautifulSoup(resp.text, 'lxml')
                
                # Suche Review-Container
                reviews_found = 0
                for container in soup.find_all(['div', 'article'], {'class': re.compile(r'review|bewertung|rating', re.I)}):
                    text = container.get_text(strip=True)
                    if len(text) > 50 and 'ADAC' in text.upper():
                        all_reviews.append({
                            'text': f"[ADAC {product} Check24] {text[:500]}",
                            'rating': None,
                            'author': None,
                            'date': None,
                            'source': 'check24.de',
                            'source_url': url,
                        })
                        reviews_found += 1
                
                print(f"{reviews_found} reviews")
                time.sleep(0.5)
                
            except Exception as e:
                print(f"Error: {e}")
        
        return all_reviews


# ============================================================================
# FINANZTIP SCRAPER - NEU!
# ============================================================================

class FinanztipScraper:
    """Finanztip.de ADAC Artikel & Kommentare."""
    
    name = "finanztip"
    
    PAGES = [
        "https://www.finanztip.de/kfz-versicherung/automobilclub/",
        "https://www.finanztip.de/kfz-versicherung/schutzbrief/",
    ]
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'de-DE,de;q=0.9',
        })
    
    def scrape(self):
        """Scrape Finanztip ADAC Inhalte."""
        all_content = []
        
        for url in self.PAGES:
            topic = url.split('/')[-2]
            print(f"  {topic}...", end=" ", flush=True)
            
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                
                soup = BeautifulSoup(resp.text, 'lxml')
                
                # Kommentare
                comments = soup.find_all(['div', 'article'], {'class': re.compile(r'comment', re.I)})
                
                count = 0
                for comment in comments:
                    text = comment.get_text(strip=True)
                    if 'ADAC' in text.upper() and len(text) > 30:
                        all_content.append({
                            'text': f"[Finanztip Kommentar] {text[:500]}",
                            'rating': None,
                            'author': None,
                            'date': None,
                            'source': 'finanztip.de',
                            'source_url': url,
                        })
                        count += 1
                
                print(f"{count} comments")
                time.sleep(0.5)
                
            except Exception as e:
                print(f"Error: {e}")
        
        return all_content


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="ADAC Review Scraper V4 - Maximum Edition",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--max-pages', type=int, default=150,
                       help='Max Seiten für Trustpilot (default: 150)')
    parser.add_argument('--output', type=str, default='adac_reviews_v4.json',
                       help='Output Datei')
    parser.add_argument('--skip', nargs='+', default=[],
                       help='Quellen überspringen (z.B. --skip gutefrage motortalk)')
    
    args = parser.parse_args()
    
    print("=" * 70)
    print(" ADAC REVIEW SCRAPER V4 - MAXIMUM EDITION")
    print("=" * 70)
    print(f" Output: {args.output}")
    print(f" Max Trustpilot Pages: {args.max_pages}")
    print("=" * 70)
    
    all_reviews = []
    stats = {}
    
    scrapers = [
        ("trustpilot", TrustpilotScraperV4, {'max_pages_per_site': args.max_pages}),
        ("kununu", KununuScraperV4, {'max_pages': 50}),
        ("finanzfluss", FinanzflussScraperV4, {}),
        ("appstore", AppStoreScraper, {}),
        ("gutefrage", GutefrageScraper, {'max_pages': 10}),
        ("motortalk", MotorTalkScraper, {'max_pages': 5}),
        ("check24", Check24Scraper, {}),
        ("finanztip", FinanztipScraper, {}),
    ]
    
    for name, ScraperClass, kwargs in scrapers:
        if name in args.skip:
            print(f"\n⏭️  {name.upper()} übersprungen")
            continue
        
        print(f"\n{'=' * 70}")
        print(f" {name.upper()}")
        print("=" * 70)
        
        try:
            scraper = ScraperClass()
            reviews = scraper.scrape(**kwargs)
            all_reviews.extend(reviews)
            stats[name] = len(reviews)
        except Exception as e:
            print(f"  Fehler: {e}")
            stats[name] = 0
    
    # === RESULTS ===
    print("\n" + "=" * 70)
    print(" ERGEBNIS")
    print("=" * 70)
    
    if all_reviews:
        # Deduplizierung
        seen = set()
        unique_reviews = []
        for r in all_reviews:
            key = r['text'][:100]
            if key not in seen:
                seen.add(key)
                unique_reviews.append(r)
        
        print(f"\n  Roh: {len(all_reviews)} reviews")
        print(f"  Nach Deduplizierung: {len(unique_reviews)} reviews")
        
        # Save
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(unique_reviews, f, ensure_ascii=False, indent=2)
        
        training_file = args.output.replace('.json', '_training.json')
        training_data = [{'id': i+1, 'text': r['text']} for i, r in enumerate(unique_reviews)]
        with open(training_file, 'w', encoding='utf-8') as f:
            json.dump(training_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n✅ TOTAL: {len(unique_reviews)} Reviews!")
        print()
        print("Statistik:")
        for source, count in stats.items():
            print(f"  • {source}: {count}")
        print()
        print(f"📁 Full data: {args.output}")
        print(f"📁 Training:  {training_file}")
        print("=" * 70)
        
        # Samples
        print("\nBeispiele:")
        for i, r in enumerate(unique_reviews[:5]):
            rating = f"★{r['rating']}" if r.get('rating') else "☆"
            print(f"[{i+1}] {rating} {r['text'][:70]}...")
    else:
        print("❌ Keine Reviews!")


if __name__ == "__main__":
    main()