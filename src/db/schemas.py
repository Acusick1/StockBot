from datetime import datetime, timedelta
from pydantic import BaseModel, model_validator, field_validator
from typing import List, Optional, Tuple, Union
from src.api.gen import get_base_period, get_delta_from_period, get_period_from_delta, split_period
from utils.hdf5 import get_h5_key
from config import settings

# All options
# valid_intervals = ("1m", "2m", "5m", "15m", "30m", "1h", "1d", "5d", "1mo", "3mo")
# valid_periods = ("1d", "5d", "1mo", "3mo", "6mo", "1y", "ytd", "2y", "5y", "10y", "max")

# Limited and more usable options
valid_intervals = ("1m", "2m", "5m", "15m", "30m", "1d", "5d")
valid_periods = ("1d", "5d", "1mo", "3mo", "6mo", "1y", "ytd", "2y", "5y", "10y")


class Interval(BaseModel):

    key: str
    delta: timedelta
    dfreq: str = "B"
    ifreq: Optional[str] = None

    @field_validator("key")
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
    interval: Optional[Interval] = Interval.from_string("1d")
    period: Optional[str] = "1y"
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None

    @field_validator("stock", mode="before")
    def validate_stock(cls, value):
        if isinstance(value, str):
            value = [value]

        return value

    @field_validator("interval", mode="before")
    def validate_interval(cls, value):
        if isinstance(value, str):
            value = Interval.from_string(value)

        return value

    @field_validator("start_date", "end_date", mode="before")
    def validate_date(cls, value):
        if isinstance(value, str):
            value = datetime.strptime(value, settings.date_fmt)

        return value

    @field_validator("period", mode="before")
    def validate_period(cls, value):
        if isinstance(value, timedelta):
            value = get_period_from_delta(value)

        return value

    @model_validator(mode="after")
    def validate(self):
        """
        Filling dependent arguments based on input.
        Only two of period, start_date, end_date required, the third can be derived.
        """

        if self.period:

            if self.period == "max":
                # Special case "max" = undefined start date
                self.start_date = None
                self.end_date = datetime.today()

            elif self.period == "ytd":
                # Special case year-to-date = start from beginning of year
                self.start_date = datetime.today().replace(month=1, day=1)
                self.end_date = datetime.today()

            elif not self.start_date:
                # If no end date provided, go with today
                self.end_date = self.end_date if self.end_date else datetime.today()
                self.start_date = self.end_date - get_delta_from_period(self.period)

            elif not self.end_date:
                self.end_date = self.start_date + get_delta_from_period(self.period)

        else:
            self.period = get_period_from_delta(self.end_date - self.start_date)

        # Start date undefined when period is max
        if self.period != "max":

            if self.start_date > self.end_date:
                raise ValueError("Start date is greater than end date!")

            elif self.end_date < self.start_date + self.interval.delta:
                raise ValueError("End date is less than start date + one interval: \n"
                                 f"{self.end_date} < {self.start_date} + {self.interval.delta}")


    def get_h5_keys(self):
        """Get key format used in h5 files: base interval (minute, daily) with stock subgroup"""
        # TODO: Match base interval to yahoo format? (minute > 1m, daily > 1d)
        base_period = get_base_period(self.interval.key)
        return [get_h5_key(base_period, stock) for stock in self.stock]

    def get_base_interval(self):
        """Get yahoo API interval format from base interval (1m or 1d)"""
        base_period = get_base_period(self.interval.key)
        return "1" + base_period[0]

    def get_h5_key(self, stock: str):

        base_period = get_base_period(self.interval.key)
        return get_h5_key(base_period, stock)


if __name__ == "__main__":

    pass
