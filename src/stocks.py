from datetime import datetime
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
    change: float = None

    def buy(self, date, value, stake):
        self.opened = True
        self.bought = Transaction(date=date, value=value, stake=stake)

    def sell(self, date, value, stake):
        self.opened = False
        self.sold = Transaction(date=date, value=value, stake=stake)
        self.change = pct_change([self.sold.value, self.bought.value])
