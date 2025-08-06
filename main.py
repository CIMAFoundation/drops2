from drops2.utils import DropsCredentials
import pandas as pd
from drops2.client import DropsCoverage

date_from =  '202507280000'
data_id = 'RISICO2023'
variable = 'V'
level = '-'

date_from = pd.Timestamp(date_from)

auth_info = DropsCredentials(
    "https://dds-test.cimafoundation.org/dds/rest", 
    ("admin", "geoDDS2013")
)
coverage = DropsCoverage(auth=auth_info)

coverage_def = coverage\
    .with_data_id(data_id)\
    .with_date(date_from)\
    .with_variable(variable)\
    .with_level(level)\

print(coverage_def.describe())

timeline = coverage_def\
    .get_timeline()

print(timeline)

data = coverage_def\
    .get_data()

print(timeline[0])