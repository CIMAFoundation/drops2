from typing import Dict, List, Tuple

from .sensors import Sensor, SensorList


class Station():
    """
    A station
    """
    name: List[str] = None 
    sensors: dict = None


class StationsList():
    """
    A list of stations
    """
    stations: List[Station] = None

    @staticmethod
    def build_stations(sensors_dict: Dict[str, SensorList]):
        return StationsList()
