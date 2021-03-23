from lib.path import data_file_path, ensure_data_dir
from lib.columns import clean_column_names
import pandas as pd
import sys
sys.path.append("../")


def clean():
    df = pd.read_csv(data_file_path(
        "ipm/new_orleans_iapro_pprr_1946-2018.csv"))
    df = df.dropna(axis=1, how="all")
    df = clean_column_names(df)
    df = df.drop(columns=["employment_number"])
    return df
