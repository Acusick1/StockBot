import pandas_market_calendars as mcal


def get_market_tz(market: str = "NYSE"):
    """
    Get timezone of market
    """
    return mcal.get_calendar(market).tz
