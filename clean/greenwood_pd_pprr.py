import deba 
import pandas as pd
from lib.columns import clean_column_names 
from lib.clean import (
    clean_races,
    clean_sexes,
    clean_dates,
    standardize_desc_cols,
    clean_names,
    clean_dates,
)
from lib import salary
from lib.uid import gen_uid

def assign_agency(df):
    df["agency"] = "greenwood-pd"
    return df

def clean_salary(df):
    df["salary"] = (
        df["salary"]
        .astype(str)
        .str.replace(" biweekly", "", regex=False)
        .str.replace(r"[$?,]", "", regex=True)
        .str.strip()
        .replace("", None)
    ) 
    df["salary_freq"] = df["salary"].apply(lambda x: salary.BIWEEKLY if pd.notna(x) else None  # Set BIWEEKLY if salary is not blank, otherwise None
    )
    df["salary"] = pd.to_numeric(df["salary"], errors='coerce') 
    return df


def clean_badge_number(df):
    df["badge_no"] = df["badge_no"].replace("?", "")
    df["badge_no"] = pd.to_numeric(df["badge_no"], errors="coerce")
    return df

def split_names(df):
    names = df.name.str.lower().str.strip().fillna("").str.split(" ", expand=True)
    df["first_name"] = names[0]
    df["last_name"] = names[names.shape[1] - 1] if names.shape[1] > 1 else ""
    df = df.drop('name', axis=1)
    return df 

def clean():
    df = (
        pd.read_csv(deba.data("raw/greenwood_pd/greenwood_pd_pprr_1990_2001_byhand.csv"))
        .pipe(clean_column_names)
        .rename(columns={"date_of_hire": "hire_date", "date_of_birth": "birth_date", "badge_number": "badge_no" })
        .pipe(clean_salary)
        .pipe(clean_badge_number)
        .pipe(clean_dates, ["hire_date", "birth_date"])
        .pipe(standardize_desc_cols, ["race", "gender"])
        .pipe(clean_races, ["race"])
        .pipe(clean_sexes, ["gender"])
        .pipe(split_names)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(assign_agency)
        .pipe(gen_uid, ["agency", "first_name", "last_name", "birth_year", "birth_month", "birth_day"])
        )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/pprr_greenwood_pd_1990_2001.csv"), index=False)


