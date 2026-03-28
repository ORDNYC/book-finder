import pandas as pd
import json
import os
from datetime import datetime

folder_path = 'data_sources'
combined_data = {}
shop_updates = {}

if not os.path.exists(folder_path):
    os.makedirs(folder_path)

for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        file_path = os.path.join(folder_path, filename)
        shop_name = filename.replace('.csv', '').replace('_', ' ').title()
        
        # Get the "As Of" date from the file itself
        mod_time = os.path.getmtime(file_path)
        shop_updates[shop_name] = datetime.fromtimestamp(mod_time).strftime('%Y-%m-%d %H:%M')
        
        try:
            df = pd.read_csv(file_path, sep=';', on_bad_lines='skip')
            df.columns = [c.strip() for c in df.columns]
            
            for _, row in df.iterrows():
                raw_isbn = str(row.get('ISBN', ''))
                isbn = raw_isbn.replace('-', '').replace(' ', '').split('.')[0]
                
                if isbn and isbn.lower() != 'nan' and len(isbn) >= 10:
                    if isbn not in combined_data:
                        combined_data[isbn] = {
                            "title": str(row.get('Book Name', 'Unknown')),
                            "availability": {}
                        }
                    
                    combined_data[isbn]["availability"][shop_name] = {
                        "price": str(row.get('Price (EUR)', '0.00')),
                        "in_stock": str(row.get('Available', 'No')).lower() == 'yes'
                    }
        except Exception as e:
            print(f"Error processing {filename}: {e}")

output = {
    "shop_dates": shop_updates, # New section!
    "books": combined_data
}

with open('books_lookup.json', 'w', encoding='utf-8') as f:
    json.dump(output, f, ensure_ascii=False, indent=4)
