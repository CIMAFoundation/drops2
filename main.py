from drops2 import sensors
from drops2.utils import DropsCredentials


data = sensors.get_sensor_data_aggr(
    'TERMOMETRO', 
    ["210331261_2", "50000359_2", "210329479_2", "50000377_2", "50000428_2"], 
    '202410010000', 
    '202410050000', 
    aggr_time=3600, 
    aggr_func='AVERAGE', 
    auth=DropsCredentials("https://dds-test.cimafoundation.org/dds/rest", ("admin", "geoDDS2013")),
    date_as_string=True, 
    as_pandas=True
)

data