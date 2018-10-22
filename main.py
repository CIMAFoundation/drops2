import click
from drops2 import sensors, coverages

if __name__ == '__main__':
    s = sensors.get_sensor_list('PLUVIOMETRO')
