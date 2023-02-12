import json
from datetime import datetime
from src.db.main import DatabaseApi
from src.stoploss import stop_losses
from utils.gen import pct_change


class Transaction:

    def __init__(self, date: datetime, value: float, stake: int):
        self.date = date
        self.value = value
        self.stake = stake


class Trade:

    opened: bool = False
    bought: Transaction = None
    sold: Transaction = None
    last_updated: tuple[datetime, float] = None
    change: float = None

    def __init__(self, ticker: str, **kwargs) -> None:
        # TODO: Validate ticker and price?
        self.ticker = ticker
        self.buy(**kwargs)

    def buy(self, date: datetime, value: float, stake: float):
        self.opened = True
        self.bought = Transaction(date=date, value=value, stake=stake)

    def sell(self, date, value, stake):
        self.opened = False
        self.sold = Transaction(date=date, value=value, stake=stake)
        self.last_updated = tuple([date, value])

    def get_change(self):
        if self.last_updated is not None:
            self.change = pct_change([self.bought.value, self.last_updated[1]])


def evaluate_trade(trade: Trade):

    db = DatabaseApi()

    # TODO: Better way of defining how much to look back
    if trade.opened:
        data = db.request([trade.ticker], period="1y")
        trade.last_updated = tuple([data.index[-1], data["Close"].iloc[-1]])

    trade.get_change()

    stops = {
        key: loss.calculate(
            data, bought_on=trade.bought.date).iloc[-1].round(3)
        for key, loss in stop_losses.items()
    }

    print(f"Bought: {trade.bought.date.date()} | {trade.bought.value}")
    print(f"Latest: {trade.last_updated[0].date()} | {trade.last_updated[1].round(3)}")
    print(f"Change: {trade.change.round(3)}%")
    print(f"Stops:", json.dumps(stops, indent=4))


if __name__ == "__main__":

    from utils.market import get_market_tz
    d = get_market_tz().localize(datetime(2023, 2, 6))

    trade = Trade("AMZN", date=d, value=102., stake=200.)
    evaluate_trade(trade)
