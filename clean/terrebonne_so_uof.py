import pandas as pd
import deba
from lib.clean import clean_datetimes, standardize_desc_cols
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid


def split_datetime_col(df):
    data = df.occurred_datetime.str.extract(r"(\w{1,2}\/\w{1,2}\/\w{4})\b ?(.+)?")
    df.loc[:, "occurred_date"] = data[0]
    df.loc[:, "occurred_time"] = data[1]
    return df.drop(columns=["occurred_datetime"])

def clean_call_type(df):
    df.loc[:, "call_type"] = df.call_type.str.lower().str.strip()\
        .str.replace(r"w\/(\w+)", r"with \1", regex=True)\
        .str.replace(r"(\w+):(\w+)", r"\1: \2", regex=True)\
        .str.replace(r"poss", "possession", regex=False)\
        .str.replace(r'att\.', "attempt", regex=True)\
        .str.replace(r"(\w)cts", r"\1 counts", regex=True)\
        .str.replace(r"\,", r"/", regex=True)\
        .str.replace(r"(\w+) ?\/ ?(\w+)", r"\1/\2", regex=True)\
        .str.replace(r"^(\w+) (\w+):", r"\1\2:", regex=True)
   
    return df 


def clean():
    df = pd.read_csv(deba.data("raw/terrebonne_so/terrebonne_so_uof_2021.csv"))\
        .pipe(clean_column_names)\
        .rename(columns={"date_time_of_incident": "occurred_datetime", "location_of_incident": "incident_location", 
        "case_number": "tracking_id", "case_status": "status"})\
        .pipe(split_datetime_col)\
        .pipe(clean_call_type)\
        .pipe(standardize_desc_cols, ["incident_location", "tracking_id", "status"])
    return df