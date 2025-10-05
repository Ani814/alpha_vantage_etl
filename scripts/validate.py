from pydantic import BaseModel, validator
from datetime import datetime

class StockRecord(BaseModel):
    date: str
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    daily_change_percentage: float

    @validator("date")
    def valid_date(cls, v):
        datetime.strptime(v, "%Y-%m-%d")
        return v

def validate_data(df):
    valid_records = []
    for _, row in df.iterrows():
        record = StockRecord(**row.to_dict())
        valid_records.append(record)
    return pd.DataFrame([r.dict() for r in valid_records])
