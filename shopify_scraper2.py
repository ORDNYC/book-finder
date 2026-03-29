import requests
import pandas as pd
import re
import time
import json
import os
from datetime import datetime
from typing import List, Dict, Optional

class BookScraper:
    def __init__(self):
        # --- CONFIGURATION ---
        self.shops = {
            'babel_books_berlin': {'url': 'https://babelbooks.de', 'currency': 'EUR', 'lookup': False},
            'nm_books': {'url': 'https://nmbooks.shop', 'currency': 'CZK', 'lookup': True},
            'belaya_vorona': {'url': 'https://vvorona.eu', 'currency': 'EUR', 'lookup': False},
            'knigomania': {'url': 'https://knigomania.org', 'currency': 'EUR', 'lookup': False},
            'muha_books': {'url': 'https://muhabooks.com', 'currency': 'EUR', 'lookup': False},
            'rewind_store': {'url': 'https://rewind-store.de', 'currency': 'EUR', 'lookup': False},
            'pishite_grishite': {'url': 'https://pishite-grishite.com', 'currency': 'ILS', 'lookup': False},
            'notre_locus': {'url': 'https://notrelocus.com', 'currency': 'GBP', 'lookup': False},
        }
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        self.cache_file = "isbn_cache.json"
        self.cache = self._load_cache()
        self.rates = self._get_exchange_rates()

    def _load_cache(self) -> Dict:
        if os.path.exists(self.cache_file):
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    def _save_cache(self):
        with open(self.cache_file, 'w', encoding='utf-8') as f:
            json.dump(self.cache, f, ensure_ascii=False, indent=2)

    def _get_exchange_rates(self) -> Dict[str, float]:
        """Fetches live EUR rates. Defaults to safe fallbacks if API is down."""
        rates = {'EUR': 1.0}
        try:
            # Using Frankfurter API (Reliable & Free)
            resp = requests.get("https://api.frankfurter.app/latest", timeout=10)
            data = resp.json().get('rates', {})
            for curr in ['CZK', 'ILS', 'GBP', 'USD']:
                if curr in data:
                    rates[curr] = 1.0 / data[curr]
            print(f"✅ Exchange rates updated (GBP: {rates.get('GBP', 1.15):.3f} EUR)")
        except Exception as e:
            print(f"⚠️ Rate fetch failed ({e}). Using fallbacks.")
            rates.update({'CZK': 0.04, 'ILS': 0.25, 'GBP': 1.18, 'USD': 0.92})
        return rates

    def clean_isbn(self, text: str) -> Optional[str]:
        if not text: return None
        digits = re.sub(r'\D', '', str(text))
        return digits if len(digits) in [10, 13] else None

    def extract_from_html(self, html: str) -> Optional[str]:
        if not html: return None
        # Priority 1: Look for ISBN labels
        label_match = re.search(r'(?:ISBN|isbn|Издание)[-:\s]*([\d\s-]{10,20})', html)
        if label_match:
            found = self.clean_isbn(label_match.group(1))
            if found: return found
        # Priority 2: Look for any 13-digit sequence starting with 978/979
        raw_match = re.search(r'\b(97[89][\d-]{10,15})\b', html)
        return self.clean_isbn(raw_match.group(1)) if raw_match else None

    def api_lookup(self, title: str) -> Optional[str]:
        if not title or title in self.cache: return self.cache.get(title)
        try:
            time.sleep(1) # Polite delay
            query = re.sub(r'[^\w\s]', '', title).strip()
            url = f"https://openlibrary.org/search.json?title={requests.utils.quote(query)}&limit=1"
            data = requests.get(url, timeout=10).json()
            if data.get('docs'):
                for i in data['docs'][0].get('isbn', []):
                    clean = self.clean_isbn(i)
                    if clean and clean.startswith('978'):
                        self.cache[title] = clean
                        return clean
        except: pass
        self.cache[title] = None
        return None

    def scrape_shop(self, shop_id: str):
        config = self.shops[shop_id]
        base_url = config['url']
        rate = self.rates.get(config['currency'], 1.0)
        results = []
        page = 1

        print(f"\n🚀 Scraping {shop_id}...")

        while True:
            try:
                resp = requests.get(f"{base_url}/products.json?page={page}&limit=250", headers=self.headers)
                if resp.status_code == 429:
                    time.sleep(30)
                    continue
                products = resp.json().get('products', [])
                if not products: break

                for p in products:
                    isbn = None
                    # PRIORITY 1: Check ALL Barcodes in variants (Fixes NM Books issue)
                    for v in p.get('variants', []):
                        isbn = self.clean_isbn(v.get('barcode'))
                        if isbn: break
                    
                    # PRIORITY 2: Check SKUs if barcode empty
                    if not isbn:
                        for v in p.get('variants', []):
                            isbn = self.clean_isbn(v.get('sku'))
                            if isbn: break
                    
                    # PRIORITY 3: HTML Description
                    if not isbn:
                        isbn = self.extract_from_html(p.get('body_html'))
                    
                    # PRIORITY 4: External API
                    if not isbn and config['lookup']:
                        isbn = self.api_lookup(p.get('title'))

                    price_eur = round(float(p['variants'][0].get('price', 0)) * rate, 2)

                    results.append({
                        'title': p.get('title'),
                        'isbn': isbn or 'N/A',
                        'price_eur': price_eur,
                        'link': f"{base_url}/products/{p.get('handle')}",
                        'shop': shop_id,
                        'updated_at': datetime.now().strftime("%Y-%m-%d")
                    })
                
                print(f"  Processed page {page}...")
                page += 1
                time.sleep(1.5)
            except: break
        
        return results

    def run(self):
        all_data = []
        for shop_id in self.shops:
            data = self.scrape_shop(shop_id)
            if data:
                pd.DataFrame(data).to_csv(f"{shop_id}.csv", index=False, encoding='utf-8-sig')
                all_data.extend(data)
                self._save_cache()
        
        if all_data:
            pd.DataFrame(all_data).to_csv("combined_results.csv", index=False, encoding='utf-8-sig')
            print(f"\n✅ Done! {len(all_data)} books total.")

if __name__ == "__main__":
    Scraper = BookScraper()
    Scraper.run()
