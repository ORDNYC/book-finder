import requests
import pandas as pd
import re
import time
import json
import os
import random
from datetime import datetime

class BookScraper:
    def __init__(self, debug=True):
        self.debug = debug
        # --- CORRECTED SHOP LIST ---
        self.shops = {
            'babel_books_berlin': {'url': 'https://babelbooksberlin.com', 'currency': 'EUR'},
            'nm_books': {'url': 'https://nmbooks.shop', 'currency': 'CZK'},
            'belaya_vorona': {'url': 'https://belayavorona.eu', 'currency': 'EUR'},
            'knigomania': {'url': 'https://knigomania.org', 'currency': 'EUR'},
            'rewind_store': {'url': 'https://rewindstore.eu', 'currency': 'EUR'},
            'notre_locus': {'url': 'https://notrelocus.com', 'currency': 'GBP'},
        }
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Referer': 'https://www.google.com/'
        }
        
        self.cache_file = "isbn_cache.json"
        self.rates = self._get_exchange_rates()

    def _get_exchange_rates(self):
        rates = {'EUR': 1.0}
        try:
            resp = requests.get("https://api.frankfurter.app/latest", timeout=10)
            data = resp.json().get('rates', {})
            for curr in ['CZK', 'GBP']:
                if curr in data: rates[curr] = 1.0 / data[curr]
            print(f"✅ Rates Updated (GBP: {rates.get('GBP', 1.18):.3f})")
        except:
            rates.update({'CZK': 0.04, 'GBP': 1.18})
        return rates

    def clean_isbn(self, text):
        if not text: return None
        digits = re.sub(r'\D', '', str(text))
        return digits if len(digits) in [10, 13] else None

    def scrape_shop(self, shop_id):
        config = self.shops[shop_id]
        base_url = config['url']
        rate = self.rates.get(config['currency'], 1.0)
        results = []
        page = 1

        print(f"\n🚀 {shop_id.upper()}")
        while True:
            try:
                time.sleep(random.uniform(1.5, 3.0)) 
                url = f"{base_url}/products.json?page={page}&limit=250"
                resp = requests.get(url, headers=self.headers, timeout=20)
                
                if resp.status_code != 200:
                    print(f"  ❌ Access Denied ({resp.status_code})")
                    break

                products = resp.json().get('products', [])
                if not products: break

                for p in products:
                    isbn = None
                    # Search ALL variants for the barcode
                    for v in p.get('variants', []):
                        isbn = self.clean_isbn(v.get('barcode')) or self.clean_isbn(v.get('sku'))
                        if isbn: break
                    
                    results.append({
                        'title': p.get('title'),
                        'isbn': isbn or 'N/A',
                        'price_eur': round(float(p['variants'][0].get('price', 0)) * rate, 2),
                        'link': f"{base_url}/products/{p.get('handle')}",
                        'shop': shop_id
                    })
                
                print(f"  Page {page} done.")
                page += 1
            except: break
        return results

    def run(self):
        all_data = []
        for shop_id in self.shops:
            all_data.extend(self.scrape_shop(shop_id))
        
        if all_data:
            pd.DataFrame(all_data).to_csv("combined_results.csv", index=False, encoding='utf-8-sig')
            print(f"\n✅ Done. Total: {len(all_data)}")

if __name__ == "__main__":
    scraper = BookScraper()
    scraper.run()
