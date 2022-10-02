from datetime import timedelta
from pandas import DataFrame, HDFStore
from unittest.mock import patch
from src.db.main import create_fake_data


@patch("yfinance.download")
def test_get_data(yfd, database_api, fake_year_request):

    database_api.get_data(request=fake_year_request)
    assert yfd.call_count == 1


# TODO: Split into two tests
# TODO: Unified way of getting h5 key from interval (or key from request)
@patch("yfinance.download")
def test_similar_requests(yfd, database_api, fake_year_request, fake_month_request, mocker):

    data = create_fake_data(request=fake_year_request)
    yfd.return_value = data

    import src.db.main
    h5_append = mocker.spy(src.db.main.pd.HDFStore, "append")

    # Two of the same calls, first should request new data, second should pull existing data
    database_api.get_data(request=fake_year_request)
    database_api.get_data(request=fake_year_request)

    # Shorter request over same period, should also pull existing data
    database_api.get_data(request=fake_month_request)

    assert h5_append.call_count == 1
    assert yfd.call_count == 1

    with HDFStore(database_api.data_file, "r") as h5:
        db_data = DataFrame(h5.get(f"/daily/{fake_month_request.stock[0]}"))
        assert db_data.shape[0] == data.shape[0]

    # Create request that should result in new download and append call
    next_day_request = fake_year_request.copy()
    next_day_request.end_date += timedelta(days=1)

    new_data = create_fake_data(request=next_day_request)
    yfd.return_value = new_data

    database_api.get_data(request=next_day_request)

    assert h5_append.call_count == 2
    assert yfd.call_count == 2

    with HDFStore(database_api.data_file, "r") as h5:
        new_db_data = DataFrame(h5.get(f"/daily/{next_day_request.stock[0]}"))
        assert new_db_data.shape[0] == new_data.shape[0]
