from lib.columns import clean_column_names
from lib.path import data_file_path, ensure_data_dir
from lib.clean import (
    clean_names, clean_dates, standardize_desc_cols
)
import pandas as pd
import sys
sys.path.append("../")


def standardize_rank_desc(df):
    rank_map = {
        'ofc.': "officer", 'cpl.': "corporal", 'sgt.': "sergeant"
    }
    df.loc[:, "rank_desc"] = df.rank_desc.map(lambda x: rank_map.get(x, ""))
    return df


def clean19():
    df = pd.read_csv(data_file_path("port_allen_pd/port_allen_cprr_2019.csv"))
    df = clean_column_names(df)
    df.columns = [
        'date_filed', 'rank_desc', 'first_name', 'last_name', 'badge_no', 'charge',
        'disposition', 'occur_date', 'occur_time', 'tracking_number']
    df = df\
        .pipe(clean_names, ["first_name", "last_name"])\
        .pipe(standardize_desc_cols, ["rank_desc", "disposition"])\
        .pipe(standardize_rank_desc)
    return df
