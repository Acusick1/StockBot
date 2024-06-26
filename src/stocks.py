import json
from datetime import datetime

from pydantic import BaseModel, field_validator, model_validator

from src.db.main import DatabaseApi
from src.stoploss import stop_losses
from src.time_db.schemas import Daily
from utils.gen import pct_change


class Trade(BaseModel):
    ticker: str
    open_stamp: datetime | None
    open_price: float
    close_stamp: datetime | None = None
    close_price: float | None = None
    quantity: float | None
    value: float | None
    type: str

    @model_validator(mode="before")
    @classmethod
    def class_validator(cls, values):
        if "value" in values:
            values["quantity"] = values["value"] / values["open_price"]

        elif "quantity" in values:
            values["value"] = values["open_price"] * values["quantity"]

        else:
            raise ValueError("Either value or quantity must be specified")

        return values

    @field_validator("type")
    @classmethod
    def type_validator(cls, trade_type):
        trade_type = trade_type.lower()

        if trade_type != "buy" and trade_type != "sell":
            raise ValueError("Trade type must be either 'buy' or 'sell'")

        return trade_type


def evaluate_trade(trade: Trade):
    db = DatabaseApi()

    # TODO: Better way of defining how much to look back
    if trade.open_stamp is not None:
        data = db.request([trade.ticker], period="1y")
        latest_price = data[Daily.adj_close].iloc[-1]
        latest_date = data.index[-1]
    else:
        raise NotImplementedError()

    change = pct_change([trade.open_price, latest_price])

    stops = {
        key: loss.calculate(data, bought_on=trade.open_stamp).iloc[-1].round(3) for key, loss in stop_losses.items()
    }

    print(f"Bought: {trade.open_stamp.date()} | {trade.open_price}")
    print(f"Latest: {latest_date.date()} | {latest_price.round(3)}")
    print(f"Change: {change.round(3)}%")
    print("Stops:", json.dumps(stops, indent=4))


if __name__ == "__main__":
    from src.time_db.schemas import nytz

    d = datetime(2023, 2, 6, tzinfo=nytz.tz)

    trade = Trade(ticker="AMZN", open_stamp=d, open_price=102.0, value=200.0, type="buy")
    evaluate_trade(trade)
