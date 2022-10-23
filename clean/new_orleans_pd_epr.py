import deba
import pandas as pd
from lib.columns import clean_column_names
from lib.clean import standardize_desc_cols

def explore_arrested(df):
    return df[~((df.offenderstatus.fillna("") == ""))]

def clean():
    df = pd.read_csv(deba.data("raw/new_orleans_pd/new_orleans_pd_epr_2010_2022.csv"))
    return df


