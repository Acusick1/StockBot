from datetime import datetime, timedelta
from pydantic import BaseModel, root_validator, validator
from typing import List, Optional, Tuple, Union
from src.api.gen import get_delta_from_period, get_period_from_delta, split_period
from config import settings


valid_intervals = ("1m", "2m", "5m", "15m", "30m", "1h", "1d", "5d", "1mo", "3mo")
valid_periods = ("1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max")


class Interval(BaseModel):

    key: str
    delta: timedelta
    dfreq: str = "B"
    ifreq: Optional[str] = None

    @validator("key")
    def validate_key(cls, value):

        if value not in valid_intervals:
            raise ValueError(f"Interval key must be one of: {valid_intervals}")

        return value

    @staticmethod
    def from_string(interval: str):

        num, unit = split_period(interval)

        if unit == "m":
            return Interval(key=interval, delta=timedelta(minutes=num), ifreq=f"{num}T")
        elif unit == "h":
            return Interval(key=interval, delta=timedelta(hours=num), ifreq=f"{num * 60}T")
        elif unit == "d":
            return Interval(key=interval, delta=timedelta(days=num), dfreq=f"{num}B")
        elif unit == "mo":
            return Interval(key=interval, delta=timedelta(days=num*30), dfreq=f"{num}M")
        elif unit == "y":
            return Interval(key=interval, delta=timedelta(days=365), dfreq=f"{num * 12}M")
        else:
            raise ValueError(f"No valid time unit found in: {interval}")


class RequestBase(BaseModel):

    stock: Union[List[str], Tuple[str]]
    interval: Interval
    period: Optional[str]
    start_date: Optional[datetime]
    end_date: Optional[datetime]

    @validator("stock", pre=True)
    def validate_stock(cls, value):
        if isinstance(value, str):
            value = [value]

        return value

    @validator("interval", pre=True)
    def validate_interval(cls, value):
        if isinstance(value, str):
            value = Interval.from_string(value)

        return value

    @validator("start_date", "end_date", pre=True)
    def validate_date(cls, value):
        if isinstance(value, str):
            value = datetime.strptime(value, settings.date_fmt)

        return value

    @validator("period", pre=True)
    def validate_period(cls, value):
        if isinstance(value, timedelta):
            value = get_period_from_delta(value)

        return value

    @root_validator
    def validate(cls, values):
        """
        Filling dependent arguments based on input.
        Only two of period, start_date, end_date required, the third can be derived.
        """
        period = values.get("period", None)
        start_date = values.get("start_date", None)
        end_date = values.get("end_date", None)
        interval = values.get("interval")

        if period:

            # Special case "max" = undefined start date
            if period == "max":
                start_date = None
                end_date = datetime.today()

            # Special case year-to-date = start from beginning of year
            if period == "ytd":
                start_date = datetime.today().replace(month=1, day=1)

            if not end_date:
                end_date = start_date + get_delta_from_period(period)
            elif not start_date:
                start_date = end_date - get_delta_from_period(period)
        else:
            period = get_period_from_delta(end_date - start_date)

        if start_date > end_date:
            raise ValueError("Start date is greater than end date!")

        elif end_date < start_date + interval.delta:
            raise ValueError("End date is less than start date + one interval: \n"
                             f"{end_date} < {start_date} + {interval.delta}")

        values.update({"period": period, "start_date": start_date, "end_date": end_date})
        return values


if __name__ == "__main__":

    pass
