from drops2 import sensors
from drops2.utils import DropsCredentials

if __name__ == '__main__':
    # s = sensors.get_sensor_list('PLUVIOMETRO')
    auth = DropsCredentials("https://dds-test.cimafoundation.org/dds/rest", ("admin", "geoDDS2013"))
    resp = sensors.get_aggregation_funtions(auth)
    print(resp)
