from typing import Dict, List, Tuple

from .sensors import Sensor, SensorList


class Station():
    name: List[str] = None 
    sensors: dict = None


class StationsList():
    stations: List[Station] = None

    @staticmethod
    def build_stations(sensors_dict: Dict[str, SensorList]):
        return StationsList()
