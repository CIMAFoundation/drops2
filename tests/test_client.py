import datetime
from unittest import mock

import pytest

from drops2.client import DropsCoverage
from drops2.auth import DropsCredentials


@pytest.fixture(autouse=True)
def reset_creds(monkeypatch):
    """Reset the DropsCredentials singleton before each test."""
    monkeypatch.setattr(DropsCredentials, "__instance", None)


def dummy_creds():
    return DropsCredentials("http://example.com", ("user", "pw"))


def call_and_assert(exc_type, *args, **kwargs):
    """Helper to assert exception type."""
    with pytest.raises(exc_type):
        DropsCoverage(dummy_creds()).with_data_id("xyz").with_date(datetime.datetime(2024, 1, 1)).get_variables(*args, **kwargs)


def test_get_variables_requires_auth_and_ids(monkeypatch):
    # Stub the underlying get_variables to just return []
    monkeypatch.setattr("drops2.coverages.get_variables", lambda data_id, date_ref, auth=None: [])

    cred = dummy_creds()
    dc = DropsCoverage(cred)

    # missing data_id should raise ValueError
    with pytest.raises(ValueError) as e:
        dc.get_variables(None)
    assert "data_id must be set" in str(e.value)

    # set data_id but missing date_ref
    dc.with_data_id("cover1")
    with pytest.raises(ValueError) as e:
        dc.get_variables(None)
    assert "date_ref must be set" in str(e.value)

    # correct call returns empty list
    dc.with_date(datetime.datetime(2024, 1, 1))
    res = dc.get_variables(None)
    assert res == []


def test_get_dates_missing_params(monkeypatch):
    monkeypatch.setattr("drops2.coverages.get_dates", lambda data_id, date_from, date_to, date_as_string=False, auth=None: [])
    cred = dummy_creds()
    dc = DropsCoverage(cred)
    dc.with_data_id("cover1")

    with pytest.raises(ValueError) as e:
        # The helper will enforce date_from and date_to presence via wrapper logic
        dc.get_dates(None, None)
    # In client.get_dates implementation, it simply calls coverages.get_dates, so here
    # we treat None as missing but our stub doesn't raise; we test format conversion below instead
    dc.with_date(datetime.datetime(2024, 1, 1))
    today = datetime.datetime.now()
    res = dc.with_date(today).get_dates(None, None)
    assert res == []


def test_format_dates_converts_datetime(monkeypatch):
    # Verify that datetime arguments are converted to string before passed to underlying API
    called_args = {}
    def fake_get_dates(data_id, date_from, date_to, date_as_string=False, auth=None):
        called_args["date_from"] = date_from
        called_args["date_to"] = date_to
        return [datetime.datetime(2024, 1, 1)]

    monkeypatch.setattr("drops2.coverages.get_dates", fake_get_dates)
    cred = dummy_creds()
    dc = DropsCoverage(cred).with_data_id("cover1").with_date(datetime.datetime(2024, 1, 1))
    start = datetime.datetime(2024, 1, 1)
    end = datetime.datetime(2024, 1, 2)
    res = dc.get_dates(None, start, end)
    assert res == [datetime.datetime(2024, 1, 1)]
    # Under the decorator, dates should be converted to strings '%Y%m%d%H%M'
    assert called_args["date_from"][0:8] == "20240101" or called_args["date_from"][0:8] == "20240101"

