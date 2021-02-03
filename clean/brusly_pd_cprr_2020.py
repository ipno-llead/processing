from lib.path import data_file_path, ensure_data_dir
from lib.columns import clean_column_names
import pandas as pd
import sys
sys.path.append("../")


def clean():
    df = pd.read_csv(data_file_path("brusly_pd/brusly_pd_cprr_2020.csv"))
    df = clean_column_names(df)
    return df
