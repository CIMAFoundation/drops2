import io
import pytz
from datetime import datetime

import numpy as np
import pandas as pd
import requests
import xarray as xr
from requests.utils import quote

from .utils import (REQUESTS_TIMEOUT, DropsCredentials, DropsException,
                    date_format, datetimes_from_strings, format_dates)


def get_supported_data():
    """
    gets a list of supported data types
    :return: list of supported data types
    """
    req_url = DropsCredentials.dds_url() + '/drops_coverages/supported/'
    response = requests.get(req_url, auth=DropsCredentials.auth_info(), timeout=REQUESTS_TIMEOUT)

    if response.status_code != 200:
        raise DropsException(
            'Error while fetching supported data',
            response=response
        )

    data = response.json()
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
    query_url = '/drops_coverages/dates/%(data_id)s/%(date_from)s/%(date_to)s/'
    query_data = dict(
        data_id=data_id,
        date_from=date_from,
        date_to=date_to
    )
    req_url = DropsCredentials.dds_url() + quote(query_url % query_data)
    
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
    query_url = '/drops_coverages/variables/%(data_id)s/%(date_ref)s/'
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
    query_url = '/drops_coverages/levels/%(data_id)s/%(date_ref)s/%(variable)s/'
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
    query_url = '/drops_coverages/timeline/%(data_id)s/%(date_ref)s/%(variable)s/%(level)s/'
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


def get_data_request(data_id, date_ref, variable, level, date_selected='all', stream=False):
    """
    get the data for the selected coverage, variable, level on the selected date and reference date
    :param data_id: coverage id
    :param date_ref: reference date
    :param variable: selected variable
    :param level: selected level
    :param date_selected: selected date
    :return: request object and request url
    """
    query_url = '/drops_coverages/coverage/%(data_id)s/%(date_ref)s/%(variable)s/%(level)s/%(date_selected)s/'
    query_data = dict(
        data_id=data_id,
        date_ref=date_ref,
        variable=variable,
        level=level,
        date_selected=date_selected
    )
    req_url = DropsCredentials.dds_url() + quote(query_url % query_data)
    print(req_url)
    response = requests.get(req_url, auth=DropsCredentials.auth_info(), timeout=REQUESTS_TIMEOUT, stream=stream)

    return response, req_url

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

    response, req_url = get_data_request(data_id, date_ref, variable, level, date_selected)
    if response.status_code != requests.codes.ok:
        raise DropsException(
            "Error while fetching data for %s - %s, variable: %s, level: %s, selected date: %s" %
            (data_id, date_ref, variable, level, date_selected),
            response=response
        )

    try:
        raw_data = io.BytesIO(response.content)
        cf_data = xr.open_dataset(raw_data)
    except Exception as exp:
        print('Error loading dataset from %s' % req_url)
        raise exp

    return cf_data


@format_dates()
def get_aggregation(data_id, date_ref, variable, level, shpfile, shpidfield, as_pandas=True):
    query_url = '/drops_coverages/aggregation/%(data_id)s/%(date_ref)s/%(variable)s/%(level)s/'
   
    query_data = dict(
        data_id=data_id,
        date_ref=date_ref,
        variable=variable,
        level=level
    )
    req_url = DropsCredentials.dds_url() + quote(query_url % query_data)

    r = requests.get(
        req_url, 
        params=dict(
            shpfile=shpfile,
            shpidfield=shpidfield
        ), 
        auth=DropsCredentials.auth_info(), 
        timeout=REQUESTS_TIMEOUT
    )
    if r.status_code != requests.codes.ok:
        raise DropsException(
            "Error while fetching aggregation for %s - %s, variable: %s, level: %s, shpfile: %s, shpid: %s" %
            (data_id, date_ref, variable, level, shpfile, shpidfield),
            response=r
        )

    try:
        data = r.json()
    except Exception as exp:
        print('Error loading dataset from %s' % req_url)
        raise exp

    if not as_pandas:
        return data
    
    dates_from, dates_to = data[0]['from'], data[0]['to']
    
    # index = [
    #     (datetime.fromtimestamp(d0/1000, tz=pytz.utc), datetime.fromtimestamp(d1/1000, tz=pytz.utc)) 
    #     for (d0,d1) in zip(dates_from, dates_to)
    # ]

    index = [
        datetime.fromtimestamp(d/1000, tz=pytz.utc) 
        for d in dates_to
    ]

    columns = [d['fid'] for d in data]
    values = np.array([d['values'] for d in data]).T
    df = pd.DataFrame(values, index=index, columns=columns)
    
    return df
