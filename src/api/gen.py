from datetime import timedelta
from dateutil.relativedelta import relativedelta


def get_delta_from_period(period: str):

    num, unit = split_period(period)

    if unit == "m":
        delta = relativedelta(minutes=num)

    elif unit == "h":
        delta = relativedelta(hours=num)

    elif unit == "d":
        delta = relativedelta(days=num)

    elif unit == "mo":
        delta = relativedelta(months=num)

    elif unit == "y":
        delta = relativedelta(years=num)
    else:
        raise ValueError(f"No valid time unit found in: {period}")

    return delta


def get_period_from_delta(delta: timedelta):

    days = delta.days

    if days > 0:

        years = days // 365
        months = days // 28

        if years:
            return f"{years}y"
        elif months:
            return f"{months}mo"
        else:
            return f"{days}d"


def split_period(period: str):
    """
    Split period string into number and unit (e.g. 30m > (30, m), 5y > (5, y))
    """
    period = period.replace(" ", "")
    unit = period.lstrip('0123456789')
    num = period.rstrip(unit)

    return int(num), unit


def get_base_period(period: str):
    # TODO: This needs to be married with the h5 file format somewhere, currently hard-coding keys

    """
    Get base period that ticker data is provided (minute or daily)
    """

    unit = split_period(period)[1]

    if unit in ["m", "h"]:
        return "minute"
    elif unit in ["d", "mo", "y"]:
        return "daily"
