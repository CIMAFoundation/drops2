from drops2 import sensors
from drops2.utils import DropsCredentials

if __name__ == '__main__':

    # data = sensors.get_sensor_data_aggr('TERMOMETRO', 
    #                                ["210331261_2", "50000359_2", "210329479_2", "50000377_2", "50000428_2"], 
    #                                '202410010000', 
    #                                '202410050000', 
    #                                aggr_time=3600, 
    #                                aggr_func='MAXIMUM', 
    #                                auth=DropsCredentials("https://dds-test.cimafoundation.org/dds/rest", ("admin", "geoDDS2013")),
    #                                date_as_string=True, 
    #                                as_pandas=False)
    
    data = sensors.get_sensor_list("PLUVIOMETRO", group="ComuneLive%PN5P", auth=DropsCredentials("https://dds-test.cimafoundation.org/dds/rest", ("admin", "geoDDS2013")))


    print(data)
