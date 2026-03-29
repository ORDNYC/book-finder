import requests
import pandas as pd
import re
import time
import json
import os
import random
from datetime import datetime
import urllib.parse

class BookScraper:
    def __init__(self):
        self.shops = {
            'babel_books_berlin': {'url': 'https://babelbooksberlin.com', 'currency': 'EUR'},
            'nm_books': {'url': 'https://nmbooks.shop', 'currency': 'CZK'},
            'belaya_vorona': {'url': 'https://belayavorona.eu', 'currency': 'EUR'},
            'knigomania': {'url': 'https://knigomania.org', 'currency': 'EUR'},
            'rewind_store': {'url': 'https://rewindstore.eu', 'currency': 'EUR'},
            'notre_locus': {'url': 'https://notrelocus.com', 'currency': 'GBP'},
        }
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        self.cache_file = "isbn_cache.json"
        self.cache = self._load_cache()
        self.rates = self._get_exchange_rates()

    def _load_cache(self):
        """Loads the memory bank from GitHub."""
        if os.path.exists(self.cache_file):
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    def _save_cache(self):
        """Saves the memory bank back to GitHub."""
        with open(self.cache_file, 'w', encoding='utf-8') as f:
            json.dump(self.cache, f, ensure_ascii=False, indent=2)

    def _get_exchange_rates(self):
        rates = {'EUR': 1.0}
        try:
            resp = requests.get("https://api.frankfurter.app/latest", timeout=10)
            data = resp.json().get('rates', {})
            for curr in ['CZK', 'GBP']:
                if curr in data: rates[curr] = 1.0 / data[curr]
        except:
            rates.update({'CZK': 0.04, 'GBP': 1.18})
        return rates

    def clean_isbn(self, text):
        if not text: return None
        digits = re.sub(r'\D', '', str(text))
        return digits if len(digits) in [10, 13] else None

    def scrape_shop(self, shop_id):
        config = self.shops[shop_id]
        results = []
        page = 1
        print(f"🚀 {shop_id.upper()}")

        while True:
            try:
                time.sleep(random.uniform(1.0, 2.0))
                url = f"{config['url']}/products.json?page={page}&limit=250"
                resp = requests.get(url, headers=self.headers, timeout=20)
                products = resp.json().get('products', [])
                if not products: break

                for p in products:
                    title = p.get('title', 'Unknown')
                    isbn = None
                    
                    # 1. Check Barcode/SKU
                    for v in p.get('variants', []):
                        isbn = self.clean_isbn(v.get('barcode')) or self.clean_isbn(v.get('sku'))
                        if isbn: break
                    
                    # 2. Check Memory Bank (The Cache)
                    if not isbn and title in self.cache:
                        isbn = self.cache[title]
                    
                    # 3. Create Smart Link
                    search_query = isbn if isbn else f"{title} ISBN"
                    google_link = f"https://www.google.com/search?q={urllib.parse.quote(search_query)}"

                    results.append({
                        'title': title,
                        'isbn': isbn or 'N/A',
                        'price_eur': round(float(p['variants'][0].get('price', 0)) * self.rates.get(config['currency'], 1.0), 2),
                        'link': f"{config['url']}/products/{p.get('handle')}",
                        'google_helper': google_link,
                        'shop': shop_id
                    })
                page += 1
            except: break
        return results

    def run(self):
        all_data = []
        for shop_id in self.shops:
            all_data.extend(self.scrape_shop(shop_id))
        
        if all_data:
            pd.DataFrame(all_data).to_csv("combined_results.csv", index=False, encoding='utf-8-sig')
            self._save_cache() # Keeps the cache alive in the repo
            print(f"✅ Finished! Total: {len(all_data)}")

if __name__ == "__main__":
    BookScraper().run()
