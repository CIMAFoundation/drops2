import requests
from requests.utils import quote
from datetime import datetime
import xarray as xr
import io

from .utils import DropsException, REQUESTS_TIMEOUT, format_dates, date_format, datetimes_from_strings
from .utils import DropsCredentials


def get_supported_data():
    """
    gets a list of supported data types
    :return: list of supported data types
    """
    req_url = DropsCredentials.dds_url() + '/drops_coverages/supported'
    r = requests.get(req_url, auth=DropsCredentials.auth_info(), timeout=REQUESTS_TIMEOUT)

    if r.status_code != 200:
        raise DropsException(
            'Error while fetching supported data',
            response=r
        )

    data = r.json()
    return data


@format_dates()
def get_dates(data_id, date_from, date_to, date_as_string=False):
    """
    gets the timeline for the selected coverage during the selected time period
    :param data_id: coverage id
    :param date_from: date from (date object or formatted string)
    :param date_to: date to (date object or formatted string)monito
    :param date_as_string: format the return values as strings instead of date objects
    :return: list of datetime objects or date strings
    """
    query_url = '/drops_coverages/dates/%(data_id)s/%(date_from)s/%(date_to)s'
    query_data = dict(
        data_id=data_id,
        date_from=date_from,
        date_to=date_to
    )
    req_url = DropsCredentials.dds_url() + quote(query_url % query_data)
    print(req_url)
    r = requests.get(req_url, auth=DropsCredentials.auth_info(), timeout=REQUESTS_TIMEOUT)

    if r.status_code is not requests.codes.ok:
        raise DropsException(
            "Error while fetching dates for %s between %s and %s" %
            (data_id, date_from, date_to),
            response=r
        )

    if date_as_string:
        dates = r.json()
    else:
        dates_str = r.json()
        dates = datetimes_from_strings(dates_str)

    return dates


@format_dates()
def get_variables(data_id, date_ref):
    """
    get the available variables for the selected coverage on the reference date
    :param data_id: coverage id
    :param date_ref: selected date
    :return: list of variables
    """
    query_url = '/drops_coverages/variables/%(data_id)s/%(date_ref)s'
    query_data = dict(
        data_id=data_id,
        date_ref=date_ref
    )

    req_url = DropsCredentials.dds_url() + quote(query_url % query_data)
    r = requests.get(req_url, auth=DropsCredentials.auth_info(), timeout=REQUESTS_TIMEOUT)

    if r.status_code is not requests.codes.ok:
        raise DropsException(
            "Error while fetching variables for %s for date %s" %
            (data_id, date_ref),
            response=r
        )

    variables = r.json()

    return variables


@format_dates()
def get_levels(data_id, date_ref, variable):
    """
    get the available levels for the selected coverage and variable on the reference date
    :param data_id: coverage id
    :param date_ref: selected date
    :param variable: selected variable
    :return: list of levels
    """
    query_url = '/drops_coverages/levels/%(data_id)s/%(date_ref)s/%(variable)s'
    query_data = dict(
        data_id=data_id,
        date_ref=date_ref,
        variable=variable
    )
    req_url = DropsCredentials.dds_url() + quote(query_url % query_data)
    r = requests.get(req_url, auth=DropsCredentials.auth_info(), timeout=REQUESTS_TIMEOUT)

    if r.status_code != requests.codes.ok:
        raise DropsException(
            "Error while fetching levels for %s - %s, variable: %s" %
            (data_id, variable, date_ref),
            response=r
        )

    levels = r.json()
    return levels


@format_dates()
def get_timeline(data_id, date_ref, variable, level, date_as_string=False):
    """
    gets the timeline of the required coverage, variable, and level on the reference date
    :param data_id: coverage id
    :param date_ref: selected date
    :param variable: selected variable
    :param level: selected level
    :param date_as_string: format the return values as strings instead of date objects
    :return: list of date objects or formatted date strings
    """
    query_url = '/drops_coverages/timeline/%(data_id)s/%(date_ref)s/%(variable)s/%(level)s'
    query_data = dict(
        data_id=data_id,
        date_ref=date_ref,
        variable=variable,
        level=level
    )

    req_url = DropsCredentials.dds_url() + quote(query_url % query_data)
    r = requests.get(req_url, auth=DropsCredentials.auth_info(), timeout=REQUESTS_TIMEOUT)
    
    if r.status_code is not requests.codes.ok:
        raise DropsException(
            "Error while fetching dates for %s - %s, variable: %s, level: %s" %
            (data_id, variable, date_ref, level),
            response=r
        )

    if date_as_string:
        dates = r.json()
    else:
        dates_str = r.json()
        dates = datetimes_from_strings(dates_str)

    return dates


@format_dates()
def get_data(data_id, date_ref, variable, level, date_selected='all'):
    """
    get the data for the selected coverage, variable, level on the selected date and reference date
    :param data_id: coverage id
    :param date_ref: reference date
    :param variable: selected variable
    :param level: selected level
    :param date_selected: selected date
    :return: a xarray dataset
    """
    query_url = '/drops_coverages/coverage/%(data_id)s/%(date_ref)s/%(variable)s/%(level)s/%(date_selected)s'
    query_data = dict(
        data_id=data_id,
        date_ref=date_ref,
        variable=variable,
        level=level,
        date_selected=date_selected
    )
    req_url = DropsCredentials.dds_url() + quote(query_url % query_data)
    print(req_url)
    r = requests.get(req_url, auth=DropsCredentials.auth_info(), timeout=REQUESTS_TIMEOUT)
    if r.status_code != requests.codes.ok:
        raise DropsException(
            "Error while fetching data for %s - %s, variable: %s, level: %s, selected date: %s" %
            (data_id, date_ref, variable, level, date_selected),
            response=r
        )

    try:
        raw_data = io.BytesIO(r.content)
        cf_data = xr.open_dataset(raw_data)
    except Exception as exp:
        print('Error loading dataset from %s' % req_url)
        raise exp


    return cf_data