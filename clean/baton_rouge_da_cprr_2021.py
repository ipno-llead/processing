from lib.columns import clean_column_names
from lib.path import data_file_path, ensure_data_dir
import pandas as pd
import sys
sys.path.append("../")


def standardize_agency(df):
    d = {
        "East Baton Rouge Sheriff's Office": "Baton Rouge SO",
        'Baton Rouge Police Department': "Baton Rouge PD"
    }
    df.loc[:, "agency"] = df.agency.map(lambda x: d.get(x, x))
    return df


def clean():
    df = pd.read_csv(data_file_path(
        "baton_rouge_da/baton_rouge_da_cprr_2021.csv"))
    df = clean_column_names(df)
    return df\
        .pipe(standardize_agency)
