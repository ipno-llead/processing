import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path
from lib.columns import clean_column_names


def clean():
    df = pd.read_csv(data_file_path('raw/west_monroe_pd/cprr_west_monroe_pd_2020_byhand.csv'))\
        .pipe(clean_column_names)
    return df
