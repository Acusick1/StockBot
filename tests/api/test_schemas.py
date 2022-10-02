import pytest
from src.api import schemas
from datetime import datetime, timedelta

today = datetime.today()
delta = timedelta(days=30)

stock_example = "EXPL"
interval_example = "1m"
period_example = "1d"


@pytest.mark.parametrize(
    "period, start_date, end_date",
    [
        (None, today - delta, today),
        (delta, today - delta, None),
        (delta, None, today)
    ]
)
def test_request_date_args(period, start_date, end_date):

    schemas.RequestBase(
        stock=stock_example,
        interval=interval_example,
        period=period,
        start_date=start_date,
        end_date=end_date
    )


def test_end_date_fail():

    with pytest.raises(ValueError, match="Start date is greater than end date"):
        schemas.RequestBase(
            stock=stock_example,
            interval=interval_example,
            start_date="2000-01-01",
            end_date="1999-01-01"
        )


def test_too_large_interval():

    with pytest.raises(ValueError):
        schemas.RequestBase(
            stock=stock_example,
            interval="5d",
            start_date="2000-01-01",
            end_date="2000-01-02",
        )
