[![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/CIMAFoundation/drops2/master?filepath=example_notebook.ipynb)

# drops2
dds data access api

## Installation

with pip:
```bash
python -m pip install --extra-index-url https://test.pypi.org/simple/ drops2
# or
python -m pip install --no-cache-dir -U git+https://github.com/CIMAFoundation/drops2.git
# you can also specify a branch or a tag
python -m pip install --no-cache-dir -U git+https://github.com/CIMAFoundation/drops2.git@v0.6.0
```


Please note that you may need to install __pyproj__ manually from the project git repository.

## Usage

The library tries to load the __.drops.rc__ file in the current folder.

Exampe of __.drops.rc__ file:
```json
{
    "dds_url": "http://example.com/dds/rest",
    "user": "admin",
    "password": "password"
}
```


If you want to programmatically set the credentials for all the calls use:
```python
import drops2
drops2.set_credentials('http://example.com/dds/rest', 'user', 'password')
```

Otherwise, you can pass the credentials to each call using the __auth__ parameter.:
```python
from drops2 import sensors
    
with DropsCredentials(url, user, password) as auth: # use the instance as a context manager
    sensors.get_sensor_classes(auth=auth)           # do something
```

#### Example
Simple example of accessing pluviometric sensors data.
```python
from drops2 import sensors

date_from = '201712110000'
date_to = '201712120000'
sensor_class = 'PLUVIOMETRO'
#require the sensor list within the geowindow
sensors_list = sensors.get_sensor_list(sensor_class, geo_win=(6.0, 36.0, 18.6, 47.5))
df_pluvio = sensors.get_sensor_data(sensor_class, sensors_list, 
                                    date_from, date_to, as_pandas=True)

```

Check out the Binder Jupyter notebook for more examples.

## Versioning

We use [SemVer](http://semver.org/) for versioning. 


## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
