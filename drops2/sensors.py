import io
import logging
from builtins import filter, map, zip  # 2 and 3 compatibility
from datetime import timedelta
from numbers import Number
from typing import Any, Dict, List, Tuple

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
    :param auth: authentication object (optional)
    :return: pandas dataframe
    """
    
    series = {}
    # check if the dataset has validSamples column
    has_valid_samples = 'validSamples' in data[0]

    for d in data:
        sensor_id = d['sensorId']
        if sensor_id in series:
            continue
        values = d['values']
        
        timeline = d['timeline']
        index = pd.to_datetime(timeline, utc=True)
        column = pd.Series(values, index=index, dtype=np.float64)        
        column = column[~column.index.duplicated(keep='first')]        
        series[sensor_id] = column
    
    df = pd.DataFrame(series)

    if has_valid_samples:
        for d in data:
            sensor_id = d['sensorId']
            timeline = d['timeline']
            index = pd.to_datetime(timeline, utc=True)
            samples = d['validSamples']
            column = pd.Series(samples, index=index, dtype=np.int32)        
            column = column[~column.index.duplicated(keep='first')]        
            df[f'{sensor_id}_samples'] = column
    
    return df

class Sensor():
    sensor_type: str
    station: int
    id: str
    name: str
    lat: float
    lng: float
    mu: str

    def __init__(self, sensor_dict):
        self.id = sensor_dict['id']
        self.station = sensor_dict['station']
        self.name = sensor_dict['stationName']
        self.lat = sensor_dict['lat']
        self.lng = sensor_dict['lon']
        self.mu = sensor_dict['sensorMU']

    def is_inside(self, geo_win) -> bool:
        inside = (geo_win[1] <= self.lat <= geo_win[3]) and (geo_win[0] <= self.lng <= geo_win[2])
        return inside

    def __repr__(self) -> str:
        return self.__dict__.__repr__()


class SensorList():
    """
    A list of sensors    
    """
    list: List[Sensor] = None
    __df: gpd.GeoDataFrame = None

    def __init__(self, sensor_list, geo_win=None):
        self.list = []
        for sensor_dict in sensor_list:
            sensor = Sensor(sensor_dict)
            if geo_win is None or sensor.is_inside(geo_win):
                self.list.append(sensor)

    def __get_by_id__(self, s_id: str) -> Sensor:
        """
        Returns the sensors with the given id
        :param s_id: sensor id
        :return: sensor
        """
        sensors_with_id = filter(lambda s: s.id==s_id, self.list)
        try:
            return next(sensors_with_id)
        except StopIteration as e:
            raise KeyError(s_id + ' not in sensor list')

    def get_by_station(self, station: int) -> Sensor:
        """
        Returns the sensors with the given station id
        :param station: station id
        :return: sensor
        """
        sensors_with_id = filter(lambda s: s.station==station, self.list)
        try:
            return next(sensors_with_id)
        except StopIteration as e:
            raise KeyError(station + ' not in sensor list')


    def __getitem__(self, item: Any) -> Sensor:
        if isinstance(item, str):
            return self.__get_by_id__(item)
        else:
            return self.list[item]

    def __repr__(self):
        return self.list.__repr__()

    def to_geopandas(self) -> gpd.GeoDataFrame:
        """
        Converts the list of sensors to a geopandas dataframe
        Stores the dataframe in a private variable to avoid multiple conversions
        :return: geopandas dataframe
        """
        if self.__df is None:
            index = [s.id for s in self.list]
            data = [(s.station, s.name, s.mu) for s in self.list]
            geometry = [Point((s.lng, s.lat)) for s in self.list]
            self.__df = gpd.GeoDataFrame(data, index=index, columns=['station', 'name', 'mu'], geometry=geometry)
            self.__df.index.name = 'id'

        return self.__df.copy()

def get_sensor_classes(auth=None):
    """
    gets a list of supported sensor classes
    :param auth: authentication object (optional)
    :return: list of supported sensor classes
    """
    if auth is None:
        auth = DropsCredentials.default()

    req_url = auth.dds_url() + '/drops_sensors/classes'
    r = requests.get(req_url, auth=auth.auth_info(), timeout=REQUESTS_TIMEOUT)

    if r.status_code is not requests.codes.ok:
        raise DropsException(
            "Error while fetching sensor classes",
            response=r
        )

    data = r.json()
    return data

def get_aggregation_functions(auth=None):
    """
    returns a list of supported aggregation funcions
    :param auth: authentication object (optional)
    :return: list of supported aggregation funcions
    """
    if auth is None:
        auth = DropsCredentials.default()

    req_url = auth.dds_url() + '/drops_sensors/aggregations'
    r = requests.get(req_url, auth=auth.auth_info(), timeout=REQUESTS_TIMEOUT)

    if r.status_code is not requests.codes.ok:
        raise DropsException(
            "Error while fetching aggregation functions",
            response=r
        )

    return r.json()

def get_sensor_list(sensor_class, group='Dewetra%Default', geo_win=None, auth=None):
    """
    gets the list of available sensors for the selected class and group
    :param sensor_class: selected sensor class
    :param group: selected group
    :param geo_win: optional geographical window for the selected sensors (lon_min, lat_min, lon_max, lat_max)
    :param auth: authentication object (optional)
    :return: list of sensors objects
    """
    if auth is None:
        auth = DropsCredentials.default()

    query_url = '/drops_sensors/anag/%(sensor_class)s/%(group)s'
    query_data = dict(
        sensor_class=sensor_class,
        group=group
    )
    req_url = auth.dds_url() + quote(query_url % query_data)
    r = requests.get(req_url, auth=auth.auth_info(), timeout=REQUESTS_TIMEOUT)

    if r.status_code is not requests.codes.ok:
        raise DropsException(
            "Error while fetching sensor anagraphic for %s on group %s" % (sensor_class, group),
            response=r
        )

    sensor_list_json = r.json()
    sensor_list = SensorList(sensor_list_json, geo_win=geo_win)
    return sensor_list

def _get_sensor_data(query_url, post_data, as_pandas, date_as_string, auth):
    req_url = auth.dds_url() + quote(query_url)
    r = requests.post(req_url, json=post_data, auth=auth.auth_info(), timeout=REQUESTS_TIMEOUT)

    data = None
    if r.status_code is not requests.codes.ok:
        raise DropsException("Error while fetching sensor data", response=r)

    data = r.json()

    if as_pandas:
        df = __raw_data_to_pandas(data)
        return df
    
    if not date_as_string:
        for sensor_data in data:
            dates_str = sensor_data['timeline']
            dates = datetimes_from_strings(dates_str)
            sensor_data['timeline'] = dates

    return data


@format_dates()
def get_sensor_data_aggr(
    sensor_class, 
    sensors, 
    date_from, 
    date_to, 
    aggr_time, 
    aggr_func, 
    date_as_string=False, 
    as_pandas=False, 
    auth=None    
):
    """
    get data from selected sensors on the selected date range, aggregating on time using the selected `aggr_func` function
    :param sensor_class: sensor class string
    :param sensors: SensorList Object, or list of Sensors or list of sensors id
    :param date_from: date from
    :param date_to: date to
    :param aggr_time: aggregation time as number of seconds or datetime.timedelta object or pd.timedelta object
    :param aggr_func: aggregation function for the dataset (returned by `get_aggregation_funtions`)
    :param date_as_string: return dates as string or datetime objects (default)
    :param as_pandas: return data converted as pandas dataframe (default)
    :param auth: authentication object (optional)
    :return: raw data as json, or pandas dataframe
    """
    if auth is None:
        auth = DropsCredentials.default()
    
    query_url = '/drops_sensors/serieaggr-smart'     
    
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

    if aggr_func:
        post_data['aggrFunction'] = aggr_func

    return _get_sensor_data(query_url, post_data, as_pandas, date_as_string, auth)

@format_dates()
def get_sensor_data(
    sensor_class, 
    sensors, 
    date_from, 
    date_to, 
    aggr_time=None, 
    date_as_string=False, 
    as_pandas=False, 
    auth=None
):
    """
    get data from selected sensors on the selected date range
    :param sensor_class: sensor class string
    :param sensors: SensorList Object, or list of Sensors or list of sensors id
    :param date_from: date from
    :param date_to: date to
    :param aggr_time: aggregation time as number of seconds or datetime.timedelta object or pd.timedelta object
    :param date_as_string: return dates as string or datetime objects (default)
    :param as_pandas: return data converted as pandas dataframe (default)
    :param auth: authentication object (optional)
    :return: raw data as json, or pandas dataframe
    """
    if auth is None:
        auth = DropsCredentials.default()
    
    if aggr_time is None:
        query_url = '/drops_sensors/serie' 
    else: 
        query_url = '/drops_sensors/serieaggr'
    
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

    
    return _get_sensor_data(query_url, post_data, as_pandas, date_as_string, auth)



def get_sensor_map_request(sensor_class, dates_selected, group,
                   cum_hours, geo_win,
                   interpolator,
                   img_dim, radius, 
                   mode=None,
                   stream=False, auth=None):
    """
    get a map for the selected sensor on the selected geowindow
    :param sensor_class: sensor class string
    :param dates_selected: array of selected dates
    :param group: group of sensors
    :param cum_hours: cumulation hours
    :param geo_win: geographical window (lon_min, lat_min, lon_max, lat_max)
    :param img_dim: dimension of the output image (nrows, ncols)
    :param radius: radius for the inrepolation function
    :param interpolator: one of 'LinearRegression', 'GRISO'
    :param mode: can be 'AVERAGE', 'MIN', 'MAX'. Works for Temperature and Relative Humidity (optional)
    :param stream: stream the response (default False)
    :param auth: authentication object (optional)
    :return: request response and url
    """
    if auth is None:
        auth = DropsCredentials.default()

    query_url = '/drops_sensors/map/'
    post_data = {
        "mapOptions": {
            "imgGeoRes": ";".join([str(f) for f in list(geo_win) + [img_dim[1], img_dim[0]]]),
            "radius": str(radius),
            "sensorClass": sensor_class,
            "raggr": group,
            "interpolator": interpolator,
        },
        "timeline": dates_selected,
        "cumh": cum_hours
    }
    if mode is not None:        
        post_data['mapOptions']['operation'] = mode

    req_url = auth.dds_url() + quote(query_url)

    response = requests.post(req_url, json=post_data, auth=auth.auth_info(), timeout=REQUESTS_TIMEOUT, stream=stream)

    return response, req_url

@format_dates(parameters=['dates_selected'])
def get_sensor_map(sensor_class, dates_selected, group='Dewetra%Default',
                   cum_hours=3, geo_win=(6.0, 36.0, 18.6, 47.5),
                   interpolator=None,
                   img_dim=(630, 575), radius=0.5,
                   mode=None,
                   auth=None):
    """
    get a map for the selected sensor class on the selected geowindow
    :param sensor_class: sensor class string
    :param dates_selected: array of selected dates
    :param group: group of sensors (default 'Dewetra%Default')
    :param cum_hours: cumulation hours (default 3)
    :param geo_win: geographical window (lon_min, lat_min, lon_max, lat_max)
    :param img_dim: dimension of the output image (nrows, ncols)
    :param radius: radius for the interpolation function: 
            if interpolator=='GRISO' => 
                radius > 0 will use Inverse Square Distance Weighting
                radius < 0 will use GRISO

    :param interpolator: one of 'LinearRegression', 'GRISO'. It defaults to 
                            'GRISO' for pluviometers, otherwise to 'LinearRegression'
    :param mode: can be 'AVERAGE', 'MIN', 'MAX'. Works for Temperature and Relative Humidity (optional)
    :param auth: authentication object (optional)                            
    :return: xarray dataset
    """

    if interpolator is None:
        interpolator = 'GRISO' if sensor_class == 'PLUVIOMETRO' else 'LinearRegression'
        

    response, req_url = get_sensor_map_request(sensor_class, dates_selected, group,
                   cum_hours, geo_win, interpolator,
                   img_dim, radius,  mode, auth=auth)

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
        logging.error('Error loading dataset from %s' % req_url)
        raise exp

    return cf_data


if __name__ == '__main__':
    pass
