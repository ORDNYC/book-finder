import requests
import pandas as pd
import re
import time
import json
import os
from datetime import datetime

class BookScraper:
    def __init__(self, debug=True):
        self.debug = debug
        # --- ALL SHOP CONFIGURATION ---
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

    def _load_cache(self):
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except: return {}
        return {}

    def _save_cache(self):
        with open(self.cache_file, 'w', encoding='utf-8') as f:
            json.dump(self.cache, f, ensure_ascii=False, indent=2)

    def _get_exchange_rates(self):
        """Fetches live EUR rates with robust GBP handling."""
        rates = {'EUR': 1.0}
        try:
            resp = requests.get("https://api.frankfurter.app/latest", timeout=10)
            data = resp.json().get('rates', {})
            for curr in ['CZK', 'ILS', 'GBP']:
                if curr in data:
                    rates[curr] = 1.0 / data[curr]
            print(f"✅ Exchange rates updated (GBP: {rates.get('GBP', 1.18):.3f} EUR)")
        except Exception as e:
            print(f"⚠️ Rate fetch failed. Using fallbacks. Error: {e}")
            rates.update({'CZK': 0.04, 'ILS': 0.25, 'GBP': 1.18})
        return rates

    def clean_isbn(self, text):
        """Strips hyphens and validates length (10 or 13)."""
        if not text: return None
        digits = re.sub(r'\D', '', str(text))
        return digits if len(digits) in [10, 13] else None

    def extract_from_html(self, html):
        """Regex fallback for ISBNs in descriptions."""
        if not html: return None
        # Look for 13-digit numbers starting with 978/979
        raw_match = re.search(r'\b(97[89][\d-]{10,15})\b', html)
        return self.clean_isbn(raw_match.group(1)) if raw_match else None

    def scrape_shop(self, shop_id):
        config = self.shops[shop_id]
        base_url = config['url']
        rate = self.rates.get(config['currency'], 1.0)
        results = []
        page = 1

        print(f"\n🚀 STARTING: {shop_id.upper()}")

        while True:
            try:
                url = f"{base_url}/products.json?page={page}&limit=250"
                resp = requests.get(url, headers=self.headers, timeout=15)
                
                if resp.status_code == 429:
                    print("  ⏳ Rate limited. Waiting 30s...")
                    time.sleep(30)
                    continue
                
                products = resp.json().get('products', [])
                if not products: break

                for p in products:
                    isbn = None
                    # 1. DEEP VARIANT SEARCH (Solves NM Books/Notre Locus issue)
                    for v in p.get('variants', []):
                        isbn = self.clean_isbn(v.get('barcode')) or self.clean_isbn(v.get('sku'))
                        if isbn: break
                    
                    # 2. HTML DESCRIPTION FALLBACK
                    if not isbn:
                        isbn = self.extract_from_html(p.get('body_html'))
                    
                    # DEBUG LOGGING
                    if not isbn and self.debug:
                        print(f"  🔍 N/A Found: {p['title'][:35]}... | Link: {base_url}/products/{p['handle']}.json")

                    price_eur = round(float(p['variants'][0].get('price', 0)) * rate, 2)

                    results.append({
                        'title': p.get('title'),
                        'isbn': isbn or 'N/A',
                        'price_eur': price_eur,
                        'shop_link': f"{base_url}/products/{p.get('handle')}",
                        'google_search_link': f"https://www.google.com/search?q={isbn}" if isbn else "N/A",
                        'shop': shop_id,
                        'date_scraped': datetime.now().strftime("%Y-%m-%d")
                    })
                
                print(f"  Page {page} complete ({len(products)} items)")
                page += 1
                time.sleep(1.2)
            except Exception as e:
                print(f"  ❌ Error on page {page}: {e}")
                break
        
        return results

    def run(self):
        all_data = []
        for shop_id in self.shops:
            shop_results = self.scrape_shop(shop_id)
            if shop_results:
                # Save individual shop CSV
                pd.DataFrame(shop_results).to_csv(f"{shop_id}.csv", index=False, encoding='utf-8-sig')
                all_data.extend(shop_results)
            self._save_cache()
        
        if all_data:
            # Save combined master file
            pd.DataFrame(all_data).to_csv("combined_results.csv", index=False, encoding='utf-8-sig')
            print(f"\n✅ SUCCESS: {len(all_data)} total books scraped.")

if __name__ == "__main__":
    # Toggle debug=False if you don't want to see missing ISBN logs
    scraper = BookScraper(debug=True)
    scraper.run()
