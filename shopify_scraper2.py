import requests
import pandas as pd
import re
import time
import json
import os
from datetime import datetime
import isbnlib

# --- CONFIGURATION ---
SHOPS = {
    'babel_books_berlin': {'url': 'https://babelbooks.de', 'currency': 'EUR', 'lookup_isbn': False},
    'nm_books': {'url': 'https://nmbooks.shop', 'currency': 'CZK', 'lookup_isbn': True},
    'belaya_vorona': {'url': 'https://vvorona.eu', 'currency': 'EUR', 'lookup_isbn': False},
    'knigomania': {'url': 'https://knigomania.org', 'currency': 'EUR', 'lookup_isbn': False},
    'muha_books': {'url': 'https://muhabooks.com', 'currency': 'EUR', 'lookup_isbn': False},
    'rewind_store': {'url': 'https://rewind-store.de', 'currency': 'EUR', 'lookup_isbn': False},
    'pishite_grishite': {'url': 'https://pishite-grishite.com', 'currency': 'ILS', 'lookup_isbn': False}
}

REQUEST_DELAY = 1.5  # Секунд между страницами
LOOKUP_DELAY = 1.0   # Секунд между запросами к Open Library
TIMEOUT = 5          # Тайм-аут для внешних API
OUTPUT_DIR = "."

# Кэш для ISBN, чтобы не искать одну и ту же книгу дважды
LOOKUP_CACHE = {}

def get_exchange_rates():
    """Получает актуальные курсы валют из ЕЦБ (к EUR)"""
    print("Fetching live exchange rates from ECB...")
    rates = {'EUR': 1.0, 'USD': 1.1} # Значения по умолчанию
    try:
        response = requests.get("https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml", timeout=10)
        for curr in ['CZK', 'ILS', 'USD']:
            match = re.search(f'currency=["\']{curr}["\']\s+rate=["\']([\d.]+)["\']', response.text)
            if match:
                rates[curr] = 1.0 / float(match.group(1))
        print(f"Rates loaded: CZK->EUR: {rates.get('CZK'):.4f}, ILS->EUR: {rates.get('ILS'):.4f}")
    except Exception as e:
        print(f"Error fetching rates: {e}. Using fallback constants.")
        rates.update({'CZK': 0.040, 'ILS': 0.25})
    return rates

def clean_isbn(text):
    """Извлекает только цифры ISBN из строки"""
    if not text: return None
    digits = re.sub(r'\D', '', str(text))
    if len(digits) in [10, 13]:
        return digits
    return None

def extract_isbn_from_html(html_content):
    """Улучшенный поиск ISBN в HTML описании товара"""
    if not html_content: return None
    
    # Очищаем от HTML тегов
    text = re.sub(r'<[^>]+>', ' ', html_content)
    
    # Ищем паттерн ISBN: 978... или ISBN 978...
    # Позволяет любые разделители между цифрами
    pattern = r'(?:ISBN|isbn|Издание)[-:\s]*([\d\s-]{10,20})'
    match = re.search(pattern, text)
    if match:
        found = clean_isbn(match.group(1))
        if found: return found
        
    # Резервный поиск: просто любая группа из 13 цифр, начинающаяся на 978/979
    fallback = re.search(r'\b(97[89][\d-]{10,15})\b', text)
    if fallback:
        return clean_isbn(fallback.group(1))
    
    return None

def open_library_lookup(title):
    """Поиск ISBN через Open Library API по названию"""
    if not title or title in LOOKUP_CACHE:
        return LOOKUP_CACHE.get(title)
    
    # Очищаем название для поиска (убираем спецсимволы)
    search_title = re.sub(r'[^\w\s]', '', title).strip()
    
    try:
        time.sleep(LOOKUP_DELAY)
        url = f"https://openlibrary.org/search.json?title={requests.utils.quote(search_title)}&limit=1"
        resp = requests.get(url, timeout=TIMEOUT)
        data = resp.json()
        
        if data.get('docs'):
            isbns = data['docs'][0].get('isbn', [])
            for i in isbns:
                clean = clean_isbn(i)
                if clean and clean.startswith('978'):
                    LOOKUP_CACHE[title] = clean
                    print(f"    > Found ISBN for '{title[:30]}...': {clean}")
                    return clean
    except Exception as e:
        print(f"    ! Lookup failed for '{title[:30]}...': {e}")
    
    LOOKUP_CACHE[title] = None
    return None

def scrape_shopify(shop_id, config, rates):
    """Основная функция скрапинга одного магазина"""
    base_url = config['url']
    currency_code = config['currency']
    rate = rates.get(currency_code, 1.0)
    all_products = []
    page = 1
    
    print(f"\nScraping: {shop_id} ({base_url})")
    
    while True:
        try:
            params = {'page': page, 'limit': 250}
            resp = requests.get(f"{base_url}/products.json", params=params, timeout=15)
            if resp.status_code != 200: break
            
            products = resp.json().get('products', [])
            if not products: break
            
            for p in products:
                title = p.get('title')
                handle = p.get('handle')
                vendor = p.get('vendor', '')
                
                # Пробуем найти ISBN всеми способами
                isbn = clean_isbn(p.get('variants', [{}])[0].get('barcode')) or \
                       clean_isbn(p.get('variants', [{}])[0].get('sku')) or \
                       extract_isbn_from_html(p.get('body_html'))
                
                # Если все еще нет ISBN и включен глубокий поиск
                if not isbn and config['lookup_isbn']:
                    isbn = open_library_lookup(title)
                
                # Данные о цене
                variant = p['variants'][0]
                price_orig = float(variant.get('price', 0))
                price_eur = round(price_orig * rate, 2)
                
                all_products.append({
                    'title': title,
                    'vendor': vendor,
                    'isbn': isbn if isbn else 'N/A',
                    'price_eur': price_eur,
                    'link': f"{base_url}/products/{handle}",
                    'shop': shop_id,
                    'updated_at': datetime.now().strftime("%Y-%m-%d")
                })
            
            print(f"  > Page {page}: {len(products)} products fetched.")
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
            # Сохраняем отдельный CSV для каждого магазина (для отладки)
            df = pd.DataFrame(shop_data)
            df.to_csv(f"{OUTPUT_DIR}/{shop_id}.csv", index=False)
            total_data.extend(shop_data)
            
    print(f"\nSuccess! Total books found: {len(total_data)}")

if __name__ == "__main__":
    main()
