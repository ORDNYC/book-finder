"""
Multi-shop Shopify book scraper
Scrapes product name, ISBN, price, and stock availability from Shopify bookstores.
Each shop gets its own CSV output file.

Prices for non-EUR shops are converted to EUR using live mid-market rates
fetched from the European Central Bank at runtime (CZK, ILS).

ISBNs are normalised to the hyphenated format (e.g. 978-5-17-089240-2).

ISBN extraction strategy (tried in order):
  1. variant.barcode field
  2. variant.sku field (if it looks like an ISBN)
  3. Regex extraction from product body_html (for shops like NM Books that
     embed the ISBN in the product description text)
  4. Open Library title lookup as last resort (if lookup_isbn=True)

Usage:
    pip install requests isbnlib
    python shopify_scraper.py
"""

import requests
import csv
import time
import os
import re
import xml.etree.ElementTree as ET
import isbnlib

# ── Configuration ─────────────────────────────────────────────────────────────

SHOPS = [
    {
        "name":        "Babel Books Berlin",
        "base_url":    "https://babelbooksberlin.com",
        "output_file": "babel_books_berlin.csv",
        "currency":    "EUR",
        "lookup_isbn": False,
    },
    {
        "name":        "Belaya Vorona",
        "base_url":    "https://belayavorona.eu",
        "output_file": "belaya_vorona.csv",
        "currency":    "CZK",
        "lookup_isbn": False,
    },
    {
        "name":        "NM Books",
        "base_url":    "https://nmbooks.shop",
        "output_file": "nm_books.csv",
        "currency":    "CZK",
        "lookup_isbn": True,   # fallback if ISBN not found in body_html either
    },
    {
        "name":        "Children of Gutenberg",
        "base_url":    "https://childrenofgutenberg.de",
        "output_file": "children_of_gutenberg.csv",
        "currency":    "EUR",
        "lookup_isbn": False,
    },
    {
        "name":        "Whale's Tales",
        "base_url":    "https://www.whales-tales.com",
        "output_file": "whales_tales.csv",
        "currency":    "EUR",
        "lookup_isbn": False,
    },
    {
        "name":        "Rewind Store",
        "base_url":    "https://rewindstore.eu",
        "output_file": "rewind_store.csv",
        "currency":    "EUR",
        "lookup_isbn": False,
    },
    {
        "name":        "BH Bookstore (Babel Haifa)",
        "base_url":    "https://bhbookstore.com",
        "output_file": "bh_bookstore.csv",
        "currency":    "ILS",
        "lookup_isbn": False,
    },
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/133.0.0.0 Safari/537.36"
    )
}

REQUEST_DELAY = 2    # seconds between page requests
PAGE_LIMIT    = 250  # max products per page (Shopify max is 250)
OUTPUT_DIR    = "."  # folder where CSV files are saved
LOOKUP_DELAY  = 1    # seconds between Open Library requests
LOOKUP_CACHE  = {}   # title -> ISBN cache

# ── ISBN helpers ───────────────────────────────────────────────────────────────

# Matches ISBN-13 and ISBN-10 with or without hyphens/spaces
ISBN_RE = re.compile(
    r'(?:ISBN[:\s\-]*)?(97[89][\s\-]?(?:\d[\s\-]?){9}\d|\d{9}[\dXx])',
    re.IGNORECASE
)


def format_isbn(raw: str) -> str:
    """Convert any ISBN string to canonical hyphenated format."""
    if not raw or raw == "N/A":
        return raw
    digits_only = re.sub(r"[^0-9Xx]", "", raw).upper()
    for candidate in [digits_only, isbnlib.to_isbn13(digits_only)]:
        if candidate and isbnlib.is_isbn13(candidate):
            masked = isbnlib.mask(candidate)
            if masked:
                return masked
    return raw


def is_valid_isbn(value: str) -> bool:
    """True if value is a recognisable ISBN-10 or ISBN-13."""
    if not value or value in ("N/A", ""):
        return False
    digits = re.sub(r"[-\s]", "", value)
    return digits.isdigit() and len(digits) in (10, 13)


def isbn_from_body(body_html: str) -> str:
    """
    Extract an ISBN from a product's HTML description using regex.
    Handles patterns like:
      ISBN: 978-1-969573-21-7
      ISBN 9781969573217
      ISBN: 978 1 969573 21 7
    Returns a formatted ISBN string or "N/A".
    """
    if not body_html:
        return "N/A"
    # Strip HTML tags first so we match plain text
    text = re.sub(r'<[^>]+>', ' ', body_html)
    match = ISBN_RE.search(text)
    if match:
        return format_isbn(match.group(0))
    return "N/A"


def isbn_from_title(title: str) -> str:
    """
    Last-resort ISBN lookup via Open Library search API.
    Results are cached to avoid duplicate requests.
    """
    if not title or title == "N/A":
        return "N/A"

    cache_key = title.strip().lower()
    if cache_key in LOOKUP_CACHE:
        return LOOKUP_CACHE[cache_key]

    try:
        response = requests.get(
            "https://openlibrary.org/search.json",
            params={"title": title, "limit": 1, "fields": "isbn"},
            headers=HEADERS,
            timeout=10,
        )
        response.raise_for_status()
        docs = response.json().get("docs", [])

        isbn = "N/A"
        if docs:
            isbns = docs[0].get("isbn", [])
            isbn13s = [i for i in isbns
                       if len(re.sub(r"[-\s]", "", i)) == 13
                       and re.sub(r"[-\s]", "", i).isdigit()]
            candidate = isbn13s[0] if isbn13s else (isbns[0] if isbns else None)
            if candidate:
                isbn = format_isbn(candidate)

        LOOKUP_CACHE[cache_key] = isbn
        time.sleep(LOOKUP_DELAY)
        return isbn

    except Exception as exc:
        print(f"    ! ISBN lookup failed for '{title}': {exc}")
        LOOKUP_CACHE[cache_key] = "N/A"
        return "N/A"


# ── Exchange rates ─────────────────────────────────────────────────────────────

def fetch_exchange_rates() -> dict:
    """Fetch live EUR exchange rates from ECB for CZK and ILS.
    Returns dict {currency_code: eur_rate} e.g. {"CZK": 0.0407, "ILS": 0.248}.
    Falls back to hardcoded rates if the feed is unavailable.
    """
    FALLBACKS = {
        "CZK": 0.0407,   # ~24.6 CZK per EUR
        "ILS": 0.248,    # ~4.03 ILS per EUR
    }
    rates = dict(FALLBACKS)  # start with fallbacks, override with live data

    try:
        response = requests.get(
            "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml",
            timeout=10
        )
        response.raise_for_status()
        root = ET.fromstring(response.content)
        for cube in root.iter("{http://www.ecb.int/vocabulary/2002-08-01/eurofxref}Cube"):
            currency = cube.get("currency")
            if currency in FALLBACKS:
                units_per_eur = float(cube.get("rate"))
                rates[currency] = 1.0 / units_per_eur
                print(f"  > Live ECB rate: 1 {currency} = {rates[currency]:.6f} EUR "
                      f"(1 EUR = {units_per_eur} {currency})")
        missing = [c for c in FALLBACKS if c not in rates or rates[c] == FALLBACKS[c]]
        if missing:
            print(f"  ! ECB feed missing {missing} -- using fallbacks")
    except Exception as exc:
        print(f"  ! ECB rate fetch failed ({exc}) -- using fallbacks for all currencies")

    return rates


# ── Core logic ─────────────────────────────────────────────────────────────────

def fetch_products(base_url: str) -> list:
    """Paginate through /products.json and return all raw product dicts."""
    all_products = []
    page = 1
    while True:
        url = f"{base_url}/products.json?limit={PAGE_LIMIT}&page={page}"
        try:
            response = requests.get(url, headers=HEADERS, timeout=20)
        except requests.RequestException as exc:
            print(f"  ! Request failed on page {page}: {exc}")
            break
        if response.status_code != 200:
            print(f"  ! HTTP {response.status_code} on page {page} -- stopping.")
            break
        products = response.json().get("products", [])
        if not products:
            break
        all_products.extend(products)
        print(f"  > Page {page}: {len(products)} products (total: {len(all_products)})")
        page += 1
        time.sleep(REQUEST_DELAY)
    return all_products


def parse_products(products: list, currency: str, exchange_rates: dict,
                   lookup_isbn: bool = False) -> list:
    """
    Extract name, ISBN, price, and availability.

    ISBN resolution order:
      1. variant.barcode
      2. variant.sku (if valid ISBN)
      3. Regex from product body_html
      4. Open Library title lookup (only if lookup_isbn=True)
    """
    rows = []
    stats = {"body": 0, "lookup": 0, "lookup_found": 0}
    eur_rate = exchange_rates.get(currency, 1.0)  # 1.0 = already EUR

    for product in products:
        title     = product.get("title", "N/A")
        body_html = product.get("body_html", "") or ""
        variants  = product.get("variants", [])

        if not variants:
            rows.append({
                "Book Name":         title,
                "ISBN":              "N/A",
                "Price (EUR)":       "N/A",
                "Original Price":    "N/A",
                "Original Currency": currency,
                "Available":         "N/A",
            })
            continue

        for variant in variants:
            isbn = "N/A"

            # 1. barcode field — may be bare number OR prefixed e.g. "ISBN: 978-5-389-21271-8"
            raw = variant.get("barcode") or ""
            if raw:
                if is_valid_isbn(raw):
                    isbn = format_isbn(raw)
                else:
                    # strip prefix like "ISBN: " and try regex extraction
                    extracted = isbn_from_body(raw)
                    if is_valid_isbn(extracted):
                        isbn = extracted

            # 2. SKU field (only if it looks like an ISBN)
            if not is_valid_isbn(isbn):
                raw = variant.get("sku") or ""
                if raw and is_valid_isbn(raw):
                    isbn = format_isbn(raw)

            # 3. Parse ISBN from product description HTML
            if not is_valid_isbn(isbn):
                isbn = isbn_from_body(body_html)
                if is_valid_isbn(isbn):
                    stats["body"] += 1

            # 4. Open Library lookup as last resort
            if not is_valid_isbn(isbn) and lookup_isbn:
                stats["lookup"] += 1
                isbn = isbn_from_title(title)
                if is_valid_isbn(isbn):
                    stats["lookup_found"] += 1
                    print(f"    > Lookup hit: '{title}' -> {isbn}")

            # Price
            raw_price = variant.get("price", "")
            try:
                price_original = float(raw_price)
            except (ValueError, TypeError):
                price_original = None

            if price_original is None:
                price_eur = "N/A"
                original_display = "N/A"
            elif currency != "EUR":
                price_eur        = f"{price_original * eur_rate:.2f}".replace(".", ",")
                original_display = f"{price_original:.2f}".replace(".", ",")
            else:
                price_eur        = f"{price_original:.2f}".replace(".", ",")
                original_display = f"{price_original:.2f}".replace(".", ",")

            # Stock
            available_raw = variant.get("available")
            available = "Yes" if available_raw is True else ("No" if available_raw is False else "N/A")

            rows.append({
                "Book Name":         title,
                "ISBN":              isbn,
                "Price (EUR)":       price_eur,
                "Original Price":    original_display,
                "Original Currency": currency,
                "Available":         available,
            })

    if stats["body"]:
        print(f"  > {stats['body']} ISBNs extracted from product descriptions")
    if lookup_isbn:
        print(f"  > Open Library lookup: {stats['lookup_found']}/{stats['lookup']} titles resolved")

    return rows


def write_csv(rows: list, filepath: str) -> None:
    fieldnames = [
        "Book Name", "ISBN", "Price (EUR)",
        "Original Price", "Original Currency", "Available",
    ]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)
    print(f"  > Saved {len(rows)} rows -> {filepath}")


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Fetching live exchange rates from ECB (CZK, ILS -> EUR)...")
    exchange_rates = fetch_exchange_rates()

    for shop in SHOPS:
        print(f"\n{'='*60}")
        print(f"Scraping: {shop['name']} ({shop['base_url']})")
        if shop["currency"] != "EUR":
            rate = exchange_rates.get(shop["currency"], "?")
            print(f"Currency: {shop['currency']} -> EUR at {rate:.6f}")
        if shop.get("lookup_isbn"):
            print("ISBN: body_html regex + Open Library fallback")
        print(f"{'='*60}")

        products = fetch_products(shop["base_url"])
        if not products:
            print(f"  ! No products found -- skipping.")
            continue

        rows = parse_products(
            products, shop["currency"], exchange_rates,
            lookup_isbn=shop.get("lookup_isbn", False),
        )
        write_csv(rows, os.path.join(OUTPUT_DIR, shop["output_file"]))

    print(f"\nDone. CSV files written to: {os.path.abspath(OUTPUT_DIR)}")


if __name__ == "__main__":
    main()
