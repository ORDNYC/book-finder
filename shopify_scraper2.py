import requests
import pandas as pd
import re
import time
import json
import os
from datetime import datetime

# --- CONFIGURATION ---
SHOPS = {
    'babel_books_berlin': {'url': 'https://babelbooks.de', 'currency': 'EUR', 'lookup_isbn': False},
    'nm_books': {'url': 'https://nmbooks.shop', 'currency': 'CZK', 'lookup_isbn': True},
    'belaya_vorona': {'url': 'https://vvorona.eu', 'currency': 'EUR', 'lookup_isbn': False},
    'knigomania': {'url': 'https://knigomania.org', 'currency': 'EUR', 'lookup_isbn': False},
    'muha_books': {'url': 'https://muhabooks.com', 'currency': 'EUR', 'lookup_isbn': False},
    'rewind_store': {'url': 'https://rewind-store.de', 'currency': 'EUR', 'lookup_isbn': False},
    'pishite_grishite': {'url': 'https://pishite-grishite.com', 'currency': 'ILS', 'lookup_isbn': False},
    'notre_locus': {'url': 'https://notrelocus.com', 'currency': 'GBP', 'lookup_isbn': False},
}

REQUEST_DELAY = 1.5  # Пауза между страницами
LOOKUP_DELAY = 1.0   # Пауза для Open Library
TIMEOUT = 10         # Тайм-аут запроса
OUTPUT_DIR = "."

# Кэш для ISBN
LOOKUP_CACHE = {}

# Заголовки, чтобы прикидываться браузером
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json'
}

def get_exchange_rates():
    """Получает курсы валют из ЕЦБ"""
    print("Fetching live exchange rates from ECB...")
    rates = {'EUR': 1.0}
    try:
        response = requests.get("https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml", timeout=TIMEOUT)
        for curr in ['CZK', 'ILS', 'USD']:
            match = re.search(f'currency=["\']{curr}["\']\s+rate=["\']([\d.]+)["\']', response.text)
            if match:
                rates[curr] = 1.0 / float(match.group(1))
        print(f"Rates loaded: CZK->EUR: {rates.get('CZK', 0):.4f}, ILS->EUR: {rates.get('ILS', 0):.4f}")
    except Exception as e:
        print(f"Error fetching rates: {e}. Using safe fallbacks.")
        rates.update({'CZK': 0.040, 'ILS': 0.25})
    return rates

def clean_isbn(text):
    """Оставляет только цифры и проверяет длину"""
    if not text: return None
    digits = re.sub(r'\D', '', str(text))
    if len(digits) in [10, 13]:
        return digits
    return None

def extract_isbn_from_html(html_content):
    """Ищет ISBN в тексте описания товара (решает проблему NM Books)"""
    if not html_content: return None
    
    # Очистка от HTML тегов
    text = re.sub(r'<[^>]+>', ' ', html_content)
    
    # Поиск по ключевому слову ISBN
    pattern = r'(?:ISBN|isbn|Издание)[-:\s]*([\d\s-]{10,20})'
    match = re.search(pattern, text)
    if match:
        found = clean_isbn(match.group(1))
        if found: return found
        
    # Резервный поиск любой строки из 13 цифр на 978/979
    fallback = re.search(r'\b(97[89][\d-]{10,15})\b', text)
    if fallback:
        return clean_isbn(fallback.group(1))
    
    return None

def open_library_lookup(title):
    """Поиск через Open Library если ISBN не найден на сайте"""
    if not title or title in LOOKUP_CACHE:
        return LOOKUP_CACHE.get(title)
    
    search_title = re.sub(r'[^\w\s]', '', title).strip()
    try:
        time.sleep(LOOKUP_DELAY)
        url = f"https://openlibrary.org/search.json?title={requests.utils.quote(search_title)}&limit=1"
        resp = requests.get(url, timeout=TIMEOUT, headers=HEADERS)
        data = resp.json()
        if data.get('docs'):
            isbns = data['docs'][0].get('isbn', [])
            for i in isbns:
                clean = clean_isbn(i)
                if clean and clean.startswith('978'):
                    LOOKUP_CACHE[title] = clean
                    return clean
    except:
        pass
    
    LOOKUP_CACHE[title] = None
    return None

def scrape_shopify(shop_id, config, rates):
    base_url = config['url']
    rate = rates.get(config['currency'], 1.0)
    all_products = []
    page = 1
    
    print(f"\nScraping: {shop_id} ({base_url})")
    
    while True:
        try:
            url = f"{base_url}/products.json?page={page}&limit=250"
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            
            if resp.status_code != 200:
                print(f"  ! Status {resp.status_code}. Stopping.")
                break
                
            products = resp.json().get('products', [])
            if not products: break
            
            for p in products:
                title = p.get('title')
                handle = p.get('handle')
                
                # 1. Пробуем Barcode -> 2. SKU -> 3. Текст описания
                isbn = clean_isbn(p.get('variants', [{}])[0].get('barcode')) or \
                       clean_isbn(p.get('variants', [{}])[0].get('sku')) or \
                       extract_isbn_from_html(p.get('body_html'))
                
                # 4. Если всё еще нет - Open Library (только если разрешено в конфиге)
                if not isbn and config['lookup_isbn']:
                    isbn = open_library_lookup(title)
                
                price_orig = float(p['variants'][0].get('price', 0))
                price_eur = round(price_orig * rate, 2)
                
                all_products.append({
                    'title': title,
                    'vendor': p.get('vendor', ''),
                    'isbn': isbn if isbn else 'N/A',
                    'price_eur': price_eur,
                    'link': f"{base_url}/products/{handle}",
                    'shop': shop_id,
                    'updated_at': datetime.now().strftime("%Y-%m-%d")
                })
            
            print(f"  > Page {page}: {len(products)} products.")
            page += 1
            time.sleep(REQUEST_DELAY)
            
        except Exception as e:
            print(f"  ! Error on page {page}: {e}")
            break
            
    return all_products

def main():
    rates = get_exchange_rates()
    total_data = []
    
    for shop_id, config in SHOPS.items():
        shop_data = scrape_shopify(shop_id, config, rates)
        if shop_data:
            pd.DataFrame(shop_data).to_csv(f"{OUTPUT_DIR}/{shop_id}.csv", index=False)
            total_data.extend(shop_data)
            
    print(f"\nFinished! Total entries: {len(total_data)}")

if __name__ == "__main__":
    main()
