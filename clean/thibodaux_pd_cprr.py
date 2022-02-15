import sys
sys.path.append("../")
import pandas as pd
from lib.path import data_file_path
from lib.columns import clean_column_names, set_values
from lib.clean import standardize_desc_cols
from lib.uid import gen_uid


def split_officer_name_columns(df):
    names = df.officers.str.lower().str.strip().str.extract(r"(\w+)\.? (\w+)")
    df.loc[:, "first_name"] = names[0]
    df.loc[:, "last_name"] = names[1]
    return df.drop(columns="officers")


def clean_recommended_disposition(df):
    df.loc[:, "recommended_disposition"] = (
        df.recommendation.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"susta?ia?ned dicipline", "sustained with discipline", regex=True)
        .str.replace(
            "sustained no dicipline", "sustained without discipline", regex=False
        )
    )
    return df.drop(columns="recommendation")


def clean_action(df):
    df.loc[:, "action"] = (
        df.discipline.str.lower()
        .str.strip()
        .str.replace("  ", " ", regex=False)
        .str.replace(
            r"demontion/5 day sus", "5-day suspension with demotion", regex=True
        )
        .str.replace(r"^no dicipline$", "", regex=True)
        .str.replace(r"w\/o", "without", regex=True)
        .str.replace(r"(\w+) days?", r"\1-day", regex=True)
        .str.replace(r"(\w+) month", r"\1-month", regex=True)
        .str.replace("no dicipline", "without discipline", regex=False)
        .str.replace("fired", "terminated", regex=False)
        .str.replace("counseled", "counseling", regex=False)
        .str.replace("two", "2", regex=False)
        .str.replace("three", "3", regex=False)
        .str.replace("one", "1", regex=False)
        .str.replace("days", "day", regex=False)
        .str.replace(r"without$", "without discipline", regex=True)
    )
    return df.drop(columns="discipline")


def clean():
    df = (
        pd.read_csv(data_file_path("raw/thibodaux_pd/thibodaux_pd_cprr_2019_2021.csv"))
        .pipe(clean_column_names)
        .rename(columns={"internal": "tracking_number"})
        .drop(columns=["external"])
        .pipe(split_officer_name_columns)
        .pipe(clean_recommended_disposition)
        .pipe(clean_action)
        .pipe(standardize_desc_cols, ["disposition"])
        .pipe(set_values, {"agency": "Thibodaux PD"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["uid", "disposition", "action", "recommended_disposition"],
            "allegation_uid",
        )
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(data_file_path("clean/cprr_thibodaux_pd_2019_2021.csv"), index=False)
