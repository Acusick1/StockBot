from pathlib import Path
from datetime import timedelta
from collections import OrderedDict

DATA_PATH = Path(Path(__file__).parent.resolve(), "data", "files")
STOCK_HISTORY_FILE = str(DATA_PATH / "stock_history.h5")

EXAMPLE_STOCKS = ['AAPL', 'F', 'MSFT', 'AMD']

MINUTE_BASE = {"name": "minute", "value": "1m"}
DAILY_BASE = {"name": "daily", "value": "1d"}


class BaseInterval:

    def __init__(self, key, value):

        self.key = key
        self.value = value


class Interval:

    def __init__(self, base: BaseInterval, delta: timedelta, dfreq: str = "B", ifreq: str = None):

        self.base = base
        self.delta = delta
        self.dfreq = dfreq
        self.ifreq = ifreq


minute_base = BaseInterval("minute", "1m")
daily_base = BaseInterval("daily", "1d")

# Anything above a day is minus one day
TIME_MAPPINGS = OrderedDict()

TIME_MAPPINGS["1m"] = Interval(base=minute_base, delta=timedelta(minutes=1), ifreq="T")
TIME_MAPPINGS["2m"] = Interval(base=minute_base, delta=timedelta(minutes=2), ifreq="2T")
TIME_MAPPINGS["5m"] = Interval(base=minute_base, delta=timedelta(minutes=5), ifreq="5T")
TIME_MAPPINGS["15m"] = Interval(base=minute_base, delta=timedelta(minutes=15), ifreq="15T")
TIME_MAPPINGS["30m"] = Interval(base=minute_base, delta=timedelta(minutes=30), ifreq="30T")
TIME_MAPPINGS["1h"] = Interval(base=minute_base, delta=timedelta(minutes=60), ifreq="60T")
TIME_MAPPINGS["1d"] = Interval(base=daily_base, delta=timedelta(days=1), dfreq="B")
TIME_MAPPINGS["5d"] = Interval(base=daily_base, delta=timedelta(days=4, seconds=1), dfreq="5B")
TIME_MAPPINGS["1mo"] = Interval(base=daily_base, delta=timedelta(days=29, seconds=1), dfreq="M")
TIME_MAPPINGS["3mo"] = Interval(base=daily_base, delta=timedelta(days=(30 * 3) - 1, seconds=1), dfreq="3M")
TIME_MAPPINGS["6mo"] = Interval(base=daily_base, delta=timedelta(days=(30 * 6) - 1, seconds=1), dfreq="3M")
TIME_MAPPINGS["1y"] = Interval(base=daily_base, delta=timedelta(days=364, seconds=1), dfreq="12M")
TIME_MAPPINGS["2y"] = Interval(base=daily_base, delta=timedelta(days=(365 * 2) - 1, seconds=1), dfreq="24M")
TIME_MAPPINGS["5y"] = Interval(base=daily_base, delta=timedelta(days=(365 * 5) - 1, seconds=1), dfreq="60M")
TIME_MAPPINGS["10y"] = Interval(base=daily_base, delta=timedelta(days=(365 * 10) - 1, seconds=1), dfreq="120M")

VALID_INTERVALS = ("1m", "2m", "5m", "15m", "30m", "1h", "1d", "5d", "1mo", "3mo")
VALID_PERIODS = ("1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max")