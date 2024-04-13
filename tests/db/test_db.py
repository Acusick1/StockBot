from datetime import timedelta
from unittest.mock import patch

from pandas import DataFrame, testing
from pandas.tseries.offsets import BDay

from src.db.main import create_fake_data


@patch("yfinance.download")
def test_get_data(yfd, database_api, fake_year_request):
    data = create_fake_data(request=fake_year_request)
    yfd.return_value = data

    database_api.get_data(request=fake_year_request)
    assert yfd.call_count == 1


def test_get_db_data(database_api, fake_year_request):
    key = fake_year_request.get_h5_keys()[0]
    put_data = create_fake_data(request=fake_year_request)
    database_api.store.put(key, put_data, format="table")

    # Pull all data, should be equal to put data
    pull_data = database_api.get_db_data(key)
    testing.assert_frame_equal(put_data, pull_data)

    # Pull data with date filters, should again be equal to put data
    pull_data = database_api.get_db_data(
        key,
        start_date=fake_year_request.start_date,
        end_date=fake_year_request.end_date,
    )

    testing.assert_frame_equal(put_data, pull_data)

    business_day_delta = BDay(5)
    start_date_delta = fake_year_request.start_date + business_day_delta
    end_date_delta = fake_year_request.end_date - business_day_delta

    pull_data = database_api.get_db_data(key, start_date=start_date_delta)
    assert pull_data.index[0].date() == start_date_delta.date()

    pull_data = database_api.get_db_data(key, end_date=end_date_delta)
    assert pull_data.index[-1].date() == end_date_delta.date()

    pull_data = database_api.get_db_data(key, start_date=start_date_delta, end_date=end_date_delta)
    assert pull_data.index[0].date() == start_date_delta.date()
    assert pull_data.index[-1].date() == end_date_delta.date()


# TODO: Split into two tests
@patch("yfinance.download")
def test_similar_requests(yfd, database_api, fake_year_request, fake_month_request, mocker):
    h5_key = fake_year_request.get_h5_keys()[0]
    data = create_fake_data(request=fake_year_request)
    yfd.return_value = data

    import src.db.main

    h5_put = mocker.spy(src.db.main.pd.HDFStore, "put")

    # Two of the same calls, first should request new data, second should pull existing data
    database_api.get_data(request=fake_year_request)
    database_api.get_data(request=fake_year_request)

    # Shorter request over same period, should also pull existing data
    database_api.get_data(request=fake_month_request)

    assert h5_put.call_count == 1
    assert yfd.call_count == 1

    db_data = DataFrame(database_api.store.get(h5_key))
    assert db_data.shape[0] == data.shape[0]

    # Create request that should result in new download and put call
    next_day_request = fake_year_request.model_copy()
    next_day_request.end_date += timedelta(days=1)

    new_data = create_fake_data(request=next_day_request)
    yfd.return_value = new_data

    database_api.get_data(request=next_day_request)

    assert h5_put.call_count == 2
    assert yfd.call_count == 2

    new_db_data = DataFrame(database_api.store.get(h5_key))
    assert new_db_data.shape[0] == new_data.shape[0]


@patch("yfinance.download")
def test_missing_rows(yfd, database_api, fake_month_request):
    h5_key = fake_month_request.get_h5_keys()[0]
    data = create_fake_data(request=fake_month_request)

    missing_rows_data = data.drop(index=data.index[-5:])
    yfd.return_value = missing_rows_data

    database_api.get_data(request=fake_month_request)

    db_data = DataFrame(database_api.store.get(h5_key))
    assert db_data.shape[0] == data.shape[0]

    # New request should not make new api call
    database_api.get_data(request=fake_month_request)
    assert yfd.call_count == 1


@patch("yfinance.download")
def test_nan_overwrite(yfd, database_api, fake_month_request):
    data = create_fake_data(request=fake_month_request)

    missing_rows_data = data.drop(index=data.index[-5:])
    yfd.return_value = missing_rows_data

    # Make request to store missing rows data
    database_api.get_data(request=fake_month_request)

    # Now return full data and make request with NaN flag
    yfd.return_value = data
    request_data = database_api.get_data(request=fake_month_request, request_nan=True)

    assert yfd.call_count == 2
    assert ~request_data.isna().to_numpy().any()
