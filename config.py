from pathlib import Path
from pydantic import BaseSettings


class Settings(BaseSettings):

    base_path: Path = Path(__file__).parent.resolve()
    data_path: Path = base_path / "data"
    stock_history_file: Path = data_path / "stock_history.h5"
    date_fmt: str = "%Y-%m-%d"

    class Config:
        env_file = Path(__file__).parent.resolve() / ".env"


class YahooApiSettings(BaseSettings):

    api_key: str
    poll_frequency: float = 0.25
    max_stocks_per_request: int = 10

    class Config:
        env_file = Path(__file__).parent.resolve() / ".env"


settings = Settings()
yahoo_api_settings = YahooApiSettings()

EXAMPLE_STOCKS = ['AAPL', 'F', 'MSFT', 'AMD', 'GOOG', 'AMZN', 'META', 'TSLA', 'NVDA', 'V', 'TSM', 'XOM', 'PG', 'JNJ']
