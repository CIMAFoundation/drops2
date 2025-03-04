import inspect
import json
from typing import Tuple
from builtins import filter, map, zip  # 2 and 3 compatibility
from datetime import date, datetime

import pytz
from decorator import decorate

date_format = '%Y%m%d%H%M'
REQUESTS_TIMEOUT = (10, 300)  # connect timeout, read timeout


class DropsCredentials:
    """
    Helper class to store the credentials for the drops webservice.
    Can be used as a context manager or singleton.
    example:

    # singleton usage
    DropsCredentials.set(url, user, password) # set the credentials in the instance
    DropsCredentials.default()                # get the instance
    sensors.get_sensor_classes() # no need to pass the auth info

    # context manager usage
    with DropsCredentials(url, (user, password)) as auth: # use the instance as a context manager
        sensors.get_sensor_classes(auth=auth)           # do something

    """
    # singleton instance
    __instance = None
    
    @staticmethod
    def load(settings_file='.drops.rc'):
        """
        Load the credentials from a file 
        """
        try:
            dds_url, auth_info = DropsCredentials.__load_settings(settings_file)
            DropsCredentials.__instance = DropsCredentials(dds_url, auth_info)
        
        except Exception as e:
            raise(e)

    @staticmethod
    def __load_settings(settings_file) -> Tuple[str, Tuple[str, str]]:
        """
        Load the credentials from a file
        """
        data = json.load(open(settings_file, 'r'))
    
        dds_url = data['dds_url']
        auth_info = data['user'], data['password']
        return dds_url, auth_info

    @staticmethod
    def set(dds_url, user, password):
        DropsCredentials.__instance = DropsCredentials(dds_url, (user, password))

    @staticmethod
    def default():
        if DropsCredentials.__instance is None:
            raise DropsLoginException("No login info was set.")
        return DropsCredentials.__instance

    
    def dds_url(self):
        """
        :return: the dds url
        """
        return self.__dds_url
    
    
    def auth_info(self):
        """
        :return: the authentication info
        """
        return self.__auth_info
    

    def __init__(self, dds_url=None, auth_info=None, *, settings_file=None):
        if dds_url is None or auth_info is None:
            if settings_file is not None:
                dds_url, auth_info = DropsCredentials.__load_settings(settings_file)                
                dds_url = dds_url
            else:
                raise DropsLoginException('Set login info or provide a settings file.')
            
        self.__dds_url = dds_url
        self.__auth_info = auth_info

    def __enter__(self):
        return self
    
    def __exit__(self, type, value, traceback):
        return True


class DropsLoginException(Exception):
    """
        Login exception
    """
    def __init__(self, message=None):
        if message is None:
            self.message = '''
            No login info files .drops.rc or ~/.drops.rc. 
            Please use DropsCredentials.set if you want to login programmatically
            '''
    
    def __str__(self):
        return repr(self)

class DropsException(Exception):
    """
        Wrapper for HTTP errors on connection to the drops webservice
    """
    response = None

    def __init__(self, message, response=None):
        """
        :param message: descriptive error message
        :param response: requests http response
        """
        self.message = message
        self.response = response

        if response is not None:
            self.message += '. Status Code: %s; Reason: %s' % (response.status_code, response.reason)

    def __str__(self):
        return repr(self.message)


def format_dates(date_format_str=date_format, parameters=None):
    """
    converts date objects to string with given format for the decorated functions
    filters by parameter name if specified
    :param date_format_str: the date format (default '%Y%m%d%H%M')
    :param parameters: list of parameters to check for conversion (default all)
    :return: the decorated function
    """

    def wrapper_func(func): 
        def wrapper(*args, **kwargs):
            # names and values as a dictionary:
            if hasattr(inspect, 'getfullargspec'):
                args_name = inspect.getfullargspec(func)[0]
            else:
                # older versions of python < 3.10
                args_name = inspect.getargspec(func)[0]
            # skip the first argument, it is the function to decorate
            args_dict = dict(zip(args_name, args[1:]))
            args_dict.update(kwargs)
            for arg_name, arg_value in args_dict.items():
                if parameters is None or any([arg_name == p for p in parameters]):
                    if type(arg_value) in (datetime, date):
                        arg_str = arg_value.strftime(date_format_str)
                        args_dict[arg_name] = arg_str

                    elif type(arg_value) in (list, tuple):
                        # check for array of dates
                        if all([type(el) in (datetime, date) for el in arg_value]):
                            arg_str_arr = [el.strftime(date_format_str) for el in arg_value]
                            args_dict[arg_name] = arg_str_arr

            return func(**args_dict)

        return decorate(func, wrapper)

    return wrapper_func 



def datetimes_from_strings(dates_str):
    return [pytz.utc.localize(datetime.strptime(d, date_format), is_dst=None) for d in dates_str]
