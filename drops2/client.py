from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Self

import numpy as np
import pandas as pd
import xarray as xr

from .auth import DropsCredentials
from .coverages import get_data, get_dates, get_levels, get_timeline, get_variables
from .utils import format_dates, DateLike, DateLikeList



@dataclass
class DropsCoverage:
    auth: DropsCredentials
    data_id: Optional[str] = None
    date_ref: Optional[DateLike] = None
    variable: Optional[str] = None
    level: Optional[str] = None

    def with_data_id(self, data_id: str) -> Self:
        self.data_id = data_id
        return self
   
    @format_dates()
    def with_date(self, date: DateLike) -> Self:
        self.date_ref = date
        return self
   
    def with_variable(self, variable: str) -> Self:
        self.variable = variable
        return self

    def with_level(self, level: str) -> Self:
        self.level = level
        return self    

    def describe(self) -> dict:
        return {
            "data_id": self.data_id,
            "date_ref": self.date_ref,
            "variable": self.variable,
            "level": self.level,
        }

    def get_variables(self, date_ref: Optional[DateLike]) -> list[str]:
        if not self.data_id:
            raise ValueError("data_id must be set before calling get_variables")
        
        date_ref = date_ref or self.date_ref
        
        if not date_ref:
            raise ValueError("date_ref must be set before calling get_variables")

        return get_variables(self.data_id, date_ref, auth=self.auth)

    def get_dates(
            self, 
            date_from: DateLike, 
            date_to: DateLike
        ) -> DateLikeList:
        if not self.data_id:
            raise ValueError("data_id must be set before calling get_dates")
        
        dates = get_dates(self.data_id, date_from, date_to, auth=self.auth)
        return dates # type: ignore

    def get_levels(
            self, 
            variable: Optional[str] = None
        ) -> list[str]:
        if not self.data_id or not self.date_ref:
            raise ValueError("data_id, date_ref must be set before calling get_levels")
            
        variable = variable or self.variable

        if not variable:
            raise ValueError("variable must be set before calling get_levels")

        return get_levels(self.data_id, self.date_ref, variable, auth=self.auth)

    def get_timeline(
            self, 
            variable: Optional[str] = None, 
            level: Optional[str] = None, 
            date_ref: Optional[DateLike] = None
        ) -> DateLikeList:
        if not self.data_id or not self.date_ref:
            raise ValueError("data_id and date_ref must be set before calling get_timeline")
        
        variable = variable or self.variable
        level = level or self.level
        date_ref = date_ref or self.date_ref

        if not variable or not level or not date_ref:
            raise ValueError("variable, level and date_ref must be set before calling get_timeline")

        return get_timeline(
            self.data_id, 
            date_ref, 
            variable, 
            level, 
            auth=self.auth
        ) # type: ignore

    def get_data(
            self, 
            date: Optional[DateLike] = None, 
            date_ref: Optional[DateLike] = None,
            variable: Optional[str] = None, 
            level: Optional[str] = None
        ) -> xr.Dataset:
        if not self.data_id or not self.date_ref:
            raise ValueError("data_id and date_ref must be set before calling get_data")
        
        variable = variable or self.variable
        level = level or self.level
        date_ref = date_ref or self.date_ref
        the_date: DateLike|str = date or 'all'

        if not variable or not level:
            raise ValueError("Both variable and level must be set before calling get_data")

        return get_data(
            data_id=self.data_id, 
            date_ref=date_ref, 
            variable=variable, 
            level=level, 
            date_selected=the_date,  # type: ignore
            auth=self.auth
        )