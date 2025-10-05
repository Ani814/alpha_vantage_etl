# scripts/etl_pipeline.py

from extract import fetch_stock_data
from transform import transform_stock_data
from load import load_data

# List of stock symbols
symbols = ["AAPL", "GOOG", "MSFT"]

for symbol in symbols:
    print(f"Running ETL for {symbol}...")
    
    # Step 1: Extract
    data = fetch_stock_data(symbol)
    if data is None:
        print(f"Skipping {symbol} due to fetch error.")
        continue
    
    # Step 2: Transform
    df = transform_stock_data(data, symbol)
    
    # Step 3: Load
    load_data(df)
    
    print(f"ETL completed for {symbol}.\n")
