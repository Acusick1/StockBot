from pathlib import Path
from pydantic import BaseSettings


class Settings(BaseSettings):

    data_path = Path(__file__).parent.resolve() / "data"
    stock_history_file = data_path / "stock_history.h5"
    date_fmt: str = "%Y-%m-%d"

    class Config:
        env_file = Path(__file__).parent.resolve() / ".env"


class YahooApiSettings(BaseSettings):

    api_key: str
    poll_frequency: float = 0.25

    class Config:
        env_file = Path(__file__).parent.resolve() / ".env"


settings = Settings()
yahoo_api_settings = YahooApiSettings()

EXAMPLE_STOCKS = ['AAPL', 'F', 'MSFT', 'AMD']
