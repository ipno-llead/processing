from lib.path import data_file_path, ensure_data_dir
from lib.columns import clean_column_names
from lib.clean import (
    float_to_int_str, clean_sexes, clean_races, standardize_desc_cols, clean_datetimes,
    clean_dates, clean_times
)
import pandas as pd
import sys
sys.path.append("../")


def initial_processing():
    df = pd.read_csv(data_file_path(
        "ipm/new_orleans_pd_cprr_actions_taken_1931-2020.csv"), escapechar="\\")
    df = df.dropna(axis=1, how="all")
    df = df.drop_duplicates()
    return clean_column_names(df)


def split_table(df):
    allegation_cols = [
        'allegation_incident_officer_id', 'allegation_primary_key', 'allegation',
        'allegation_finding', 'allegation_final_disposition', 'allegation_created_date',
        'allegation_year_created', 'allegation_month_created', 'allegation_finding_date',
        'allegation_created_on', 'allegation_final_disposition_date', 'allegation_directive'
    ]
    allegations = df[allegation_cols].drop_duplicates().dropna(how="all")\
        .dropna(subset=["allegation_primary_key"])

    action_taken_cols = ['allegation_primary_key']+[
        col for col in df.columns
        if col not in allegation_cols
    ]
    action_takens = df[action_taken_cols].drop_duplicates().dropna(how="all")\
        .dropna(subset=["action_primary_key"])
    return allegations, action_takens


def clean():
    df = initial_processing()
    allegations, action_takens = split_table(df)
    return allegations, action_takens
