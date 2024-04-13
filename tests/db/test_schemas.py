import pytest
from src.db import schemas
from datetime import datetime, timedelta

today = datetime.today()
delta = timedelta(days=30)

stock_example = "EXPL"
interval_example = "1m"
period_example = "1d"


@pytest.mark.parametrize(
    "period, start_date, end_date",
    [(None, today - delta, today), (delta, today - delta, None), (delta, None, today)],
)
def test_request_date_args(period, start_date, end_date):
    schemas.RequestBase(
        stock=stock_example,
        interval=interval_example,
        period=period,
        start_date=start_date,
        end_date=end_date,
    )


@pytest.mark.parametrize("period", ["ytd"])
def test_periods(period):
    schemas.RequestBase(
        stock=stock_example, interval=interval_example, period=period, end_date=today
    )


@pytest.mark.parametrize(
    "interval_string, total_seconds",
    [
        ("1m", 60),
        ("2m", 2 * 60),
        ("5m", 5 * 60),
        ("15m", 15 * 60),
        ("30m", 30 * 60),
        ("1d", 24 * 60 * 60),
        # ("1h", 60*60), ("5d", 5*24*60*60), ("1mo", 30*24*60*60), ("3mo", 3*30*24*60*60)
    ],
)
def test_interval_from_string(interval_string, total_seconds):
    interval = schemas.Interval.from_string(interval_string)
    assert interval.delta.total_seconds() == total_seconds


def test_invalid_interval():
    with pytest.raises(ValueError, match="must be one of"):
        schemas.Interval.from_string("6m")


def test_end_date_fail():
    with pytest.raises(ValueError, match="Start date is greater than end date"):
        schemas.RequestBase(
            stock=stock_example,
            interval=interval_example,
            start_date="2000-01-01",
            end_date="1999-01-01",
        )


def test_too_large_interval():
    with pytest.raises(ValueError):
        schemas.RequestBase(
            stock=stock_example,
            interval="5d",
            start_date="2000-01-01",
            end_date="2000-01-01",
        )
