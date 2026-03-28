import pandas as pd
import json
import os

# 1. Point to your folder of CSVs
folder_path = 'data_sources'
combined_data = {}
all_shop_names = []

# 2. Iterate through every file in that folder
for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        # Use filename as Shop Name (e.g., "babel_books" instead of "babel_books.csv")
        shop_name = filename.replace('.csv', '').replace('_', ' ').title()
        all_shop_names.append(shop_name)
        
        try:
            # Read the file
            df = pd.read_csv(os.path.join(folder_path, filename), sep=';')
            df.columns = [c.strip() for c in df.columns] # Clean column names
            
            for _, row in df.iterrows():
                isbn = str(row.get('ISBN', '')).replace('-', '').replace(' ', '').split('.')[0]
                
                if len(isbn) >= 10:
                    if isbn not in combined_data:
                        combined_data[isbn] = {
                            "title": row.get('Book Name'),
                            "availability": {}
                        }
                    
                    combined_data[isbn]["availability"][shop_name] = {
                        "price": row.get('Price (EUR)'),
                        "in_stock": str(row.get('Available', 'No')).lower() == 'yes'
                    }
        except Exception as e:
            print(f"Error processing {filename}: {e}")

# 3. Save the data AND the list of shops found
output = {
    "shops": all_shop_names,
    "books": combined_data
}

with open('books_lookup.json', 'w', encoding='utf-8') as f:
    json.dump(output, f, ensure_ascii=False, indent=4)

print(f"Success! Processed {len(all_shop_names)} shops and {len(combined_data)} books.")