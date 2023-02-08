import deba
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.clean import clean_dates, clean_names, standardize_desc_cols
from lib.rows import duplicate_row
import re
from lib.uid import gen_uid

def strip_aps(df):
    df = df.apply(lambda x: x.replace(r"^\'", "", regex=True))
    return df 

def split_names(df):
    names = df.name.str.lower().str.strip()\
        .str.extract(r"(.+)\, (.+)")
    
    df.loc[:, "last_name"] = names[0]
    df.loc[:, "first_name"] = names[1]
    return df.drop(columns=["name"])

def clean_action(df):
    df.loc[:, "action"] = df.disciplinary_action.str.lower().str.strip()\
        .str.replace(r"(\w+) $", r"\1", regex=True)\
        .str.replace(r"(\w+) hour", r"\1-hour", regex=True)\
        .str.replace(r"(\w+) day", r"\1-day", regex=True)\
        .str.replace(r"\/", r"; ", regex=True)
    return df.drop(columns=["disciplinary_action"])


def split_rows_with_multiple_allegations(df):
    i = 0
    for idx in df[df.allegation.str.contains("/")].index:
        s = df.loc[idx + i, "allegation"]
        parts = re.split(r"\s*(?:\/)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "allegation"] = name
        i += len(parts) - 1
    return df


def clean():
    df = pd.read_csv(deba.data("raw/iberia_so/iberia_so_cprr_2018_2021.csv"))\
        .pipe(clean_column_names)\
        .rename(columns={"date": "receive_date", "dispo": "disposition"})\
        .pipe(strip_aps)\
        .pipe(split_names)\
        .pipe(clean_dates, ["receive_date"])\
        .pipe(clean_names, ["first_name", "last_name"])\
        .pipe(clean_action)\
        .pipe(split_rows_with_multiple_allegations)\
        .pipe(standardize_desc_cols, ["disposition", "allegation", "action"])\
        .pipe(set_values, {"agency": "iberia-so"})\
        .pipe(gen_uid, ["first_name", "last_name", "agency"])\
        .pipe(gen_uid, ["allegation", "action", "disposition", "uid"], "allegation_uid")
    return df 


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/iberia_so_cprr_2019_2021.csv"), index=False)