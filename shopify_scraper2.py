import requests
import pandas as pd
import re
import time
from datetime import datetime

class BookScraper:
    def __init__(self, debug=True):
        self.debug = debug
        self.shops = {
            'notre_locus': {'url': 'https://notrelocus.com', 'currency': 'GBP', 'lookup': False},
            'nm_books': {'url': 'https://nmbooks.shop', 'currency': 'CZK', 'lookup': True},
            # ... add your other shops here ...
        }
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

    def clean_isbn(self, text):
        if not text: return None
        digits = re.sub(r'\D', '', str(text))
        return digits if len(digits) in [10, 13] else None

    def scrape_shop(self, shop_id):
        config = self.shops[shop_id]
        results = []
        page = 1

        print(f"\n🚀 SCRAPING: {shop_id}")
        while True:
            url = f"{config['url']}/products.json?page={page}&limit=250"
            resp = requests.get(url, headers=self.headers).json()
            products = resp.get('products', [])
            if not products: break

            for p in products:
                isbn = None
                # Check every variant (Essential for Notre Locus/NM Books)
                for v in p.get('variants', []):
                    isbn = self.clean_isbn(v.get('barcode'))
                    if isbn: break
                
                # If N/A but debug is on, we print the link to investigate
                if not isbn and self.debug:
                    print(f"  🔍 Missing ISBN: {p['title'][:30]}... -> {config['url']}/products/{p['handle']}.json")

                results.append({
                    'title': p.get('title'),
                    'isbn': isbn or 'N/A',
                    'price_raw': p['variants'][0].get('price'),
                    'link': f"{config['url']}/products/{p['handle']}",
                    # This link forces a search even if the shop hides the book
                    'google_books_link': f"https://www.google.com/search?tbm=bks&q={isbn}" if isbn else "N/A",
                    'shop': shop_id
                })
            
            print(f"  Page {page} processed.")
            page += 1
            time.sleep(1)
        return results

    def run(self):
        all_data = []
        for shop_id in self.shops:
            all_data.extend(self.scrape_shop(shop_id))
        
        pd.DataFrame(all_data).to_csv("combined_results.csv", index=False, encoding='utf-8-sig')
        print(f"\n✅ Scraping finished. Total: {len(all_data)}")

if __name__ == "__main__":
    scraper = BookScraper(debug=True)
    scraper.run()
