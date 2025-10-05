import requests
import json
from datetime import datetime

API_KEY = "YOUR_API_KEY"  # Replace with your Alpha Vantage API key
BASE_URL = "https://www.alphavantage.co/query"

def fetch_stock_data(symbol):
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()

    # Save raw JSON
    date_str = datetime.now().strftime("%Y-%m-%d")
    with open(f"../raw_data/{symbol}_{date_str}.json", "w") as f:
        json.dump(data, f, indent=4)

    return data
