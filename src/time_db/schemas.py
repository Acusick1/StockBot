from datetime import datetime
from pydantic import BaseModel


class StockBase(BaseModel):

    ticker: str


class DailyBase(BaseModel):

    stock_id: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    adj_close: float
    volume: int