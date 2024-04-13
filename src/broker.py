import csv
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path

from src.db.main import DatabaseApi
from src.stocks import Trade
from utils.market import get_market_tz


class Broker(ABC):
    def __init__(self, cash_balance: float):
        self.cash_balance = cash_balance
        self.open_positions: list[Trade] = []
        self.closed_positions: list[Trade] = []
        self.trades_csv_file = "trades.csv"

    def buy(self, ticker: str, value: float, **kwargs) -> Trade | None:
        trade = self._buy(ticker, value, **kwargs)

        if trade is not None:
            self.cash_balance -= value
            self.open_positions.append(trade)
            self._write_trade_to_csv(trade)

        return trade

    def close(self, trade: Trade, **kwargs):
        if trade not in self.open_positions:
            raise ValueError("Trade not found in open positions")

        elif trade.open_price is None:  # noqa: RET506
            raise ValueError("Trade is not open!")

        closed_trade = self._close(trade, **kwargs)
        self._update_trade_in_csv(trade)
        self.open_positions.remove(trade)
        self.closed_positions.append(closed_trade)

        return closed_trade

    def sell(self, ticker: str, value: float, **kwargs) -> Trade | None:
        trade = self._sell(ticker, value, **kwargs)
        if trade is not None:
            for open_trade in self.open_positions:
                # Selling first trade of given ticker
                if open_trade.ticker == ticker:
                    open_trade.close_price = trade.price
                    open_trade.close_stamp = get_market_tz().localize(datetime.now(tz=timezone.utc))
                    self.closed_positions.append(open_trade)
                    self.open_positions.remove(open_trade)
                    self.cash_balance += value
                    self._update_trade_in_csv(open_trade)
                    break
        return trade

    def get_open_positions(self) -> list[Trade]:
        return self.open_positions

    def get_closed_positions(self) -> list[Trade]:
        return self.closed_positions

    def get_cash_balance(self) -> float:
        return self.cash_balance

    @abstractmethod
    def _buy(self, ticker: str, value: float, **kwargs) -> Trade | None:
        raise NotImplementedError()

    @abstractmethod
    def _sell(self, ticker: str, value: float, **kwargs) -> Trade | None:
        raise NotImplementedError()

    @abstractmethod
    def _close(self, trade: Trade, **kwargs) -> Trade | None:
        raise NotImplementedError()

    def _write_trade_to_csv(self, trade: Trade):
        with Path(self.trades_csv_file).open("a") as trades_file:
            writer = csv.writer(trades_file)
            if trades_file.tell() == 0:
                writer.writerow(trade.model_dump().keys())
            writer.writerow(trade.model_dump().values())

    def _update_trade_in_csv(self, trade: Trade):
        temp_file = "temp.csv"

        with Path(self.trades_csv_file).open("r") as trades_file, Path(temp_file).open("w", newline="") as f:
            reader = csv.model_dumpReader(trades_file)
            writer = csv.model_dumpWriter(f, fieldnames=reader.fieldnames)
            writer.writeheader()
            for row in reader:
                if (
                    row["ticker"] == trade.ticker
                    and row["open_price"] == str(trade.open_price)
                    and row["close_price"] == ""
                ):
                    row["close_price"] = str(trade.close_price)
                    row["close_stamp"] = str(trade.close_stamp)
                writer.writerow(row)

        Path(temp_file).replace(self.trades_csv_file)


class TestBroker(Broker):
    db = DatabaseApi()

    def __init__(self, cash_balance: float):
        super().__init__(cash_balance)

    def _align_date(self, d: datetime):
        # TODO: Abstract
        d = get_market_tz().localize(d)
        return d.replace(hour=0, minute=0)

    def _buy(self, ticker: str, value: float, d: datetime, price: float | None = None) -> Trade:
        d = self._align_date(d)

        data = self.db.request(stock=[ticker])

        if price is None:
            price = data.loc[d, "Close"]
        elif not (data.loc[d, "Low"] <= price <= data.loc[d, "High"]):
            raise ValueError(
                f"Input price: {price} is outside of bounds (Low: {data.loc[d, 'Low']} High: {data.loc[d, 'High']})\
                for date: {d.date().strftime('%Y-%m-%d')}"
            )

        return Trade(ticker=ticker, open_price=price, value=value, open_stamp=d, type="buy")

    def _close(self, trade: Trade, d: datetime, price: float | None = None) -> Trade:
        d = self._align_date(d)

        data = self.db.request(stock=[trade.ticker])

        if price is None:
            price = data.loc[d, "Close"]
        elif not (data.loc[d, "Low"] <= price <= data.loc[d, "High"]):
            raise ValueError(
                f"Input price: {price} is outside of bounds (Low: {data.loc[d, 'Low']} High: {data.loc[d, 'High']})\
                for date: {d.date().strftime('%Y-%m-%d')}"
            )

        trade.close_price = price
        trade.close_stamp = d

        return trade

    def _sell(self):
        pass


if __name__ == "__main__":
    broker = TestBroker(cash_balance=1000)
    _ = broker.buy("AMZN", value=200.0, d=datetime(year=2023, month=2, day=6, tzinfo=timezone.utc))
    trade = broker.close(broker.open_positions[0], d=datetime(year=2023, month=2, day=17, tzinfo=timezone.utc))
    print(trade)
