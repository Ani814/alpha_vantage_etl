import pandas as pd
from datetime import datetime

def transform_stock_data(raw_json, symbol):
    ts_data = raw_json.get("Time Series (Daily)", {})
    
    df = pd.DataFrame(ts_data).T
    df.reset_index(inplace=True)
    df.rename(columns={
        "index": "date",
        "1. open": "open_price",
        "2. high": "high_price",
        "3. low": "low_price",
        "4. close": "close_price",
        "5. volume": "volume"
    }, inplace=True)
    
    # Convert types
    df["open_price"] = df["open_price"].astype(float)
    df["high_price"] = df["high_price"].astype(float)
    df["low_price"] = df["low_price"].astype(float)
    df["close_price"] = df["close_price"].astype(float)
    df["volume"] = df["volume"].astype(int)
    
    # Calculate daily change percentage
    df["daily_change_percentage"] = ((df["close_price"] - df["open_price"]) / df["open_price"]) * 100
    df["symbol"] = symbol
    df["extraction_timestamp"] = datetime.now()
    
    return df
