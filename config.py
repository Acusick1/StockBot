from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="allow")

    base_path: Path = Path(__file__).parent.resolve()
    data_path: Path = base_path / "data"
    date_fmt: str = "%Y-%m-%d"
    debug: bool = False
    db_username: str
    db_password: str
    db_host: str
    db_port: int
    db_name: str


class YahooApiSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    api_key: str
    poll_frequency: float = 0.25
    max_stocks_per_request: int = 10


settings = Settings()
yahoo_api_settings = YahooApiSettings()

EXAMPLE_STOCKS = [
    "AAPL",
    "F",
    "MSFT",
    "AMD",
    "GOOG",
    "AMZN",
    "META",
    "TSLA",
    "NVDA",
    "V",
    "TSM",
    "XOM",
    "PG",
    "JNJ",
]
