from pathlib import Path

DATA_PATH = Path(Path(__file__).parent.resolve(), "data", "files")
STOCK_HISTORY_FILE = str(DATA_PATH / "stock_history.h5")

EXAMPLE_STOCKS = ['AAPL', 'F']

VALID_PERIODS = ("1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max")
VALID_INTERVALS = ("1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo")
TIME_IN_SECONDS = {
    "1m": 60, "2m": 60 * 2, "5m": 60 * 5, "15m": 60 * 15, "30m": 60 * 30, "60m": 60 * 60, "90m": 60 * 90, "1h": 60 * 60,
    "1d": 60 * 60 * 24, "5d": 60 * 60 * 24 * 5, "1wk": 60 * 60 * 24 * 7, "1mo": 60 * 60 * 24 * 30,
    "3mo": 60 * 60 * 24 * 30 * 3, "6mo": 60 * 60 * 24 * 30 * 6, "1y": 60 * 60 * 24 * (365 - 1),
    "2y": 60 * 60 * 24 * ((365 * 2) - 1), "5y": 60 * 60 * 24 * ((365 * 5) - 1), "10y": 60 * 60 * 24 * ((365 * 10) - 1)
}
