import io
from builtins import filter, map, zip  # 2 and 3 compatibility
from datetime import timedelta
from typing import Any, Dict, List, Tuple
from numbers import Number

import geopandas as gpd
import numpy as np
import pandas as pd
import requests
import xarray as xr
from pandas import Timedelta
from requests.utils import quote
from shapely.geometry import Point

from .utils import (REQUESTS_TIMEOUT, DropsCredentials, DropsException,
                    datetimes_from_strings, format_dates)


def __raw_data_to_pandas(data):
    """
    converts the json data from the server to a dataframe
    :param data: list of stations data
    :return: pandas dataframe
    """
    df = pd.DataFrame()
    for d in data:
        column = pd.Series(d['values'], index=d['timeline'])
        #column = column.drop_duplicates(keep='last')
        column.index = pd.to_datetime(column.index)
        df[d['sensorId']] = column
        # try:
        #     df[d['sensorId']] = column
        # except ValueError as ve:
        #     df = df[df.index.drop_duplicates()]
        #     df[d['sensorId']] = column

    df.index = pd.to_datetime(df.index)
    return df

class Sensor():
    sensor_type: str = None
    id: str = None
    name: str = None
    lat: float = None
    lng: float = None
    mu: str = None

    def __init__(self, sensor_dict):
        self.id = sensor_dict['id']
        self.name = sensor_dict['stationName']
        self.lat = sensor_dict['lat']
        self.lng = sensor_dict['lon']
        self.mu = sensor_dict['sensorMU']

    def is_inside(self, geo_win) -> bool:
        inside = (geo_win[2] <= self.lat <= geo_win[3]) and (geo_win[0] <= self.lng <= geo_win[1])
        return inside

    def __repr__(self) -> str:
        return self.__dict__.__repr__()


class SensorList():
    list: List[Sensor] = None
    __df: gpd.GeoDataFrame = None

    def __init__(self, sensor_list, geo_win=None):
        self.list = []
        for sensor_dict in sensor_list:
            sensor = Sensor(sensor_dict)
            if geo_win is None or sensor.is_inside(geo_win):
                self.list.append(sensor)

    def __get_by_id__(self, s_id: str) -> Sensor:
        sensors_with_id = filter(lambda s: s.id==s_id, self.list)
        try:
            return next(sensors_with_id)
        except StopIteration as e:
            raise KeyError(s_id + ' not in sensor list')

    def __getitem__(self, item: Any) -> Sensor:
        if isinstance(item, str):
            return self.__get_by_id__(item)
        else:
            return self.list[item]

    def __repr__(self):
        return self.list.__repr__()

    def to_geopandas(self) -> gpd.GeoDataFrame:
        if self.__df is None:
            index = [s.id for s in self.list]
            data = [(s.name, s.mu) for s in self.list]
            geometry = [Point((s.lng, s.lat)) for s in self.list]
            self.__df = gpd.GeoDataFrame(data, index=index, columns=['name', 'mu'], geometry=geometry)
            self.__df.index.name = 'id'

        return self.__df.copy()

def get_sensor_classes():
    """
    gets a list of supported sensor classes
    :return: list of supported sensor classes
    """
    req_url = DropsCredentials.dds_url() + '/drops_sensors/classes'
    r = requests.get(req_url, auth=DropsCredentials.auth_info(), timeout=REQUESTS_TIMEOUT)

    if r.status_code is not requests.codes.ok:
        raise DropsException(
            "Error while fetching sensor classes",
            response=r
        )

    data = r.json()
    return data


def get_sensor_list(sensor_class, group='Dewetra%Default', geo_win=None):
    """
    gets the list of available sensors for the selected class and group
    :param sensor_class: selected sensor class
    :param group: selected group
    :param geo_win: optional geographical window for the selected sensors (lonmin, lonmax, latmin, latmax)
    :return: list of sensors objects
    """
    query_url = '/drops_sensors/anag/%(sensor_class)s/%(group)s'
    query_data = dict(
        sensor_class=sensor_class,
        group=group
    )
    req_url = DropsCredentials.dds_url() + quote(query_url % query_data)
    r = requests.get(req_url, auth=DropsCredentials.auth_info(), timeout=REQUESTS_TIMEOUT)

    if r.status_code is not requests.codes.ok:
        raise DropsException(
            "Error while fetching sensor anagraphic for %s on group %s" % (sensor_class, group),
            response=r
        )

    sensor_list_json = r.json()
    sensor_list = SensorList(sensor_list_json, geo_win=geo_win)
    return sensor_list


@format_dates()
def get_sensor_data(sensor_class, sensors, date_from, date_to, aggr_time=None, date_as_string=False, as_pandas=False):
    """
    get data from selected sensors on the selected date range
    :param sensor_class: sensor class string
    :param sensors: SensorList Object, or list of Sensors or list of sensors id
    :param date_from: date from
    :param date_to: date to
    :param aggr: aggregation time as number of seconds or datetime.timedelta object or pd.timedelta object
    :param date_as_string: return dates as string or datetime objects (default)
    :param as_pandas: return data converted as pandas dataframe (default)
    :return:
    """
    query_url = '/drops_sensors/serieaggr' if aggr_time else '/drops_sensors/serie'
    
    if type(sensors) is SensorList:
        sensors = sensors.list

    if all([type(s) is Sensor for s in sensors]):
        id_sensors = [s.id for s in sensors]
    elif all([type(s) is str for s in sensors]):
        id_sensors = sensors
    else:
        raise DropsException("sensor list not valid")

    post_data = {
        'sensorClass': sensor_class,
        'from': date_from,
        'to': date_to,
        'ids': id_sensors
    }

    if aggr_time:
        if type(aggr_time) in (timedelta, Timedelta):
            aggr_seconds = aggr_time.total_seconds()
        elif isinstance(aggr_time, Number):
            aggr_seconds = aggr_time
        else:
            raise DropsException(f'aggr_time object is neither numeric or timedelta object [{aggr_time}]')

        post_data['step'] = aggr_seconds

    req_url = DropsCredentials.dds_url() + quote(query_url)
    r = requests.post(req_url, json=post_data, auth=DropsCredentials.auth_info(), timeout=REQUESTS_TIMEOUT)

    data = None
    if r.status_code is not requests.codes.ok:
        raise DropsException(
            "Error while fetching sensor data for %s between %s and %s" %
            (sensor_class, date_from, date_to),
            response=r
        )

    data = r.json()

    if as_pandas:
        df = __raw_data_to_pandas(data)
        return df
    else:
        if not date_as_string:
            for sensor_data in data:
                dates_str = sensor_data['timeline']
                dates = datetimes_from_strings(dates_str)
                sensor_data['timeline'] = dates

        return data


def __get_sensor_map_request(sensor_class, dates_selected, group,
                   cum_hours, geo_win,
                   img_dim, radius):
    """
    get a map for the selected sensor on the selected geowindow
    :param sensor_class: sensor class string
    :param dates_selected: array of selected dates
    :param group: group of sensors
    :param cum_hours: cumulation hours
    :param geo_win: geographical window (lon_min, lon_max, lat_min, lat_max)
    :param img_dim: dimension of the output image (nrows, ncols)
    :param radius: radius for the inrepolation function
    :return: request response and url
    """
    query_url = '/drops_sensors/map/'
    post_data = {
        "mapOptions": {
            "imgGeoRes": ";".join([str(f) for f in geo_win + img_dim]),
            "radius": str(radius),
            "sensorClass": sensor_class,
            "raggr": group
        },
        "timeline": dates_selected,
        "cumh": cum_hours
    }

    req_url = DropsCredentials.dds_url() + quote(query_url)

    response = requests.post(req_url, json=post_data, auth=DropsCredentials.auth_info(), timeout=REQUESTS_TIMEOUT)

    return response, req_url

@format_dates(parameters=['dates_selected'])
def get_sensor_map(sensor_class, dates_selected, group='Dewetra%Default',
                   cum_hours=3, geo_win=(6.0, 36.0, 18.6, 47.5),
                   img_dim=(630, 575), radius=0.5):
    """
    get a map for the selected sensor on the selected geowindow
    :param sensor_class: sensor class string
    :param dates_selected: array of selected dates
    :param group: group of sensors (default 'Dewetra%Default')
    :param cum_hours: cumulation hours (default 3)
    :param geo_win: geographical window (lon_min, lon_max, lat_min, lat_max)
    :param img_dim: dimension of the output image (nrows, ncols)
    :param radius: radius for the inrepolation function
    :return: xarray dataset
    """
    response, req_url = __get_sensor_map_request(sensor_class, dates_selected, group,
                   cum_hours, geo_win,
                   img_dim, radius)

    if response.status_code is not requests.codes.ok:
        raise DropsException(
            "Error while fetching data for %s" %
            (sensor_class,),
            response=response
        )

    try:
        raw_data = io.BytesIO(response.content)
        cf_data = xr.open_dataset(raw_data)
    except Exception as exp:
        print('Error loading dataset from %s' % req_url)
        raise exp


    return cf_data


if __name__ == '__main__':
    pass
