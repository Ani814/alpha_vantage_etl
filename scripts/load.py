# scripts/load.py

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# -------------------------
# Update these variables
# -------------------------
DB_USER = "user" #Username and password have been removed for security reasons
DB_PASSWORD = "password"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "stock_db"

# Create connection engine
engine = create_engine(
    f"postgresql+psycopg2://username:password@localhost:5432/stock_db"
)

# Table name
TABLE_NAME = "stock_daily_data"

def create_table_if_not_exists():
    """
    Create the table if it doesn't exist.
    """
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        symbol VARCHAR(10) NOT NULL,
        date DATE NOT NULL,
        open_price FLOAT,
        high_price FLOAT,
        low_price FLOAT,
        close_price FLOAT,
        volume BIGINT,
        daily_change_percentage FLOAT,
        extraction_timestamp TIMESTAMP NOT NULL,
        UNIQUE(symbol, date)
    );
    """
    try:
        with engine.connect() as conn:
            conn.execute(text(create_table_query))
            conn.commit()
        print(f"Table '{TABLE_NAME}' is ready.")
    except SQLAlchemyError as e:
        print(f"Error creating table: {e}")

def load_data(df: pd.DataFrame):
    """
    Load transformed DataFrame into PostgreSQL.
    Avoid inserting duplicate records for the same symbol and date.
    """
    if df.empty:
        print("DataFrame is empty. Nothing to load.")
        return

    create_table_if_not_exists()

    try:
        with engine.begin() as conn:
            for _, row in df.iterrows():
                insert_query = f"""
                INSERT INTO {TABLE_NAME} 
                (symbol, date, open_price, high_price, low_price, close_price, volume, daily_change_percentage, extraction_timestamp)
                VALUES (:symbol, :date, :open_price, :high_price, :low_price, :close_price, :volume, :daily_change_percentage, :extraction_timestamp)
                ON CONFLICT (symbol, date) DO NOTHING;
                """
                conn.execute(text(insert_query), row.to_dict())
        print(f"Loaded {len(df)} rows into '{TABLE_NAME}'.")
    except SQLAlchemyError as e:
        print(f"Error loading data: {e}")
