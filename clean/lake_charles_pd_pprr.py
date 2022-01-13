import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path
from lib.columns import clean_column_names


def clean():
    df = pd.read_csv(data_file_path('raw/lake_charles_pd/pprr_lake_charles_pd_06_14_2021.csv'))\
        .pipe(clean_column_names)
    return df 
