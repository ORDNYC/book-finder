import pandas as pd
import json
import os
from datetime import datetime

folder_path = 'data_sources'
combined_data = {}
shop_updates = {}

# Ensure the folder exists
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

# Process every CSV in the data_sources folder
for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        file_path = os.path.join(folder_path, filename)
        
        # Create a clean Shop Name from filename (e.g., 'babel_books' -> 'Babel Books')
        shop_name = filename.replace('.csv', '').replace('_', ' ').title()
        
        # Get the "As Of" date from the file's metadata
        mod_time = os.path.getmtime(file_path)
        shop_updates[shop_name] = datetime.fromtimestamp(mod_time).strftime('%Y-%m-%d %H:%M')
        
        try:
            # Semicolon is standard for your exports
            df = pd.read_csv(file_path, sep=';', on_bad_lines='skip')
            df.columns = [c.strip() for c in df.columns]
            
            for _, row in df.iterrows():
                raw_isbn = str(row.get('ISBN', ''))
                # Clean ISBN: remove dashes, spaces, and decimals
                isbn = raw_isbn.replace('-', '').replace(' ', '').split('.')[0]
                
                if isbn and isbn.lower() != 'nan' and len(isbn) >= 10:
                    if isbn not in combined_data:
                        combined_data[isbn] = {
                            "title": str(row.get('Book Name', 'Unknown Title')),
                            "availability": {}
                        }
                    
                    # Add this shop's specific info to the book
                    combined_data[isbn]["availability"][shop_name] = {
                        "price": str(row.get('Price (EUR)', '0.00')),
                        "in_stock": str(row.get('Available', 'No')).lower() == 'yes'
                    }
        except Exception as e:
            print(f"Error processing {filename}: {e}")

# Save in the format the new HTML expects
output = {
    "shop_dates": shop_updates,
    "books": combined_data
}

with open('books_lookup.json', 'w', encoding='utf-8') as f:
    json.dump(output, f, ensure_ascii=False, indent=4)

print(f"Successfully updated books_lookup.json with {len(combined_data)} books.")
