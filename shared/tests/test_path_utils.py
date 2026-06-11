from shared.path_utils import is_url


def test_is_url_detects_http_and_https():
    assert is_url("http://example.com/data.csv")
    assert is_url("https://raw.githubusercontent.com/org/repo/main/data.csv")


def test_is_url_rejects_local_paths():
    assert not is_url("/app/data/regions.csv")
    assert not is_url("data/regions.csv")
    assert not is_url("~/Downloads/regions.csv")
    assert not is_url("C:\\Users\\me\\regions.csv")


def test_is_url_handles_non_string():
    assert not is_url(None)
