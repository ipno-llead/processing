import deba 
import pandas as pd
from lib.columns import clean_column_names
from lib.clean import (
    clean_races,
    clean_sexes,
    clean_dates,
    standardize_desc_cols,
    clean_names,
)
from lib.uid import gen_uid
from lib.columns import set_values


def clean_salary(df):
    df["salary"] = (
        df["salary"]
        .astype(str)
        .str.replace(" biweekly", "", regex=False)
        .str.replace(r"[$?,]", "", regex=True)  # Removes $, ?, and ,
        .str.strip()
        .replace("", None)  # Convert empty strings to None
    )
    df["salary"] = pd.to_numeric(df["salary"], errors="coerce")  # Convert to numeric
    return df

def clean_badge_number(df):
    df["badge_number"] = df["badge_number"].replace("?", None)
    df["badge_number"] = pd.to_numeric(df["badge_number"], errors="coerce")
    return df

def split_names(df):
    names = df.name.str.lower().str.strip().fillna("").str.split(" ", expand=True)
    df.loc[:, "first_name"] = names[0]
    df.loc[:, "middle_name"] = names[1] if names.shape[1] > 1 else ""
    df.loc[:, "last_name"] = names[names.shape[1] - 1]  # Last word as last name
    return df.drop(columns="name")

def clean_dates(df, date_cols):
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")  # Converts invalid dates to NaT
    return df

def clean():
    df = (
        pd.read_csv(deba.data("raw/greenwood_pd_pprr_1990_2001_byhand.csv"))
        .pipe(clean_column_names)
        .rename(columns={"date_of_hire": "hire_date", "date_of_birth": "dob"})
        .pipe(clean_salary)
        .pipe(clean_badge_number)
        .pipe(clean_dates, ["hire_date", "dob"])
        .pipe(standardize_desc_cols, ["race", "gender"])
        .pipe(clean_races, ["race"])
        .pipe(clean_sexes, ["gender"])
        .pipe(split_names)
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(set_values, {"agency": "new-agency"})
        .pipe(gen_uid, ["first_name", "middle_name", "last_name", "dob"])
    )
    return df

if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/greenwood_pd_pprr_1990_2001_byhand.csv"), index=False)


#new try 

import pandas as pd 
import deba

data = pd.read_csv('raw/greenwood_pd_pprr_1990_2001_byhand.csv')

