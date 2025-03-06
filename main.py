from drops2 import sensors, coverages
from drops2.utils import DropsCredentials
s = sensors.get_sensor_list('TERMOMETRO')
s.as_serializable()

sensors.get_aggregation_functions('TERMOMETRO', auth=DropsCredentials("https://dds-test.cimafoundation.org/dds/rest", ("admin", "geoDDS2013")))

data = sensors.get_sensor_data_aggr(
    'TERMOMETRO', 
    s.list, 
    '202410010000', 
    '202410050000', 
    aggr_time=3600, 
    aggr_func='AVERAGE', 
    auth=DropsCredentials("https://dds-test.cimafoundation.org/dds/rest", ("admin", "geoDDS2013")),
    date_as_string=True, 
    as_pandas=True
)



