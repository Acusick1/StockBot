def test_single_request(yf_api, single_request):
    data = yf_api.make_request(request=single_request)
    assert data.shape[0] > 0


def test_multi_request(yf_api, multi_request):
    data = yf_api.make_request(request=multi_request)
    assert data.shape[0] > 0
