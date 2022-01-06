import sys

sys.path.append("../")
from lib.path import data_file_path
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.clean import clean_dates, clean_names
from lib.uid import gen_uid


def extract_names(df):
    names = (
        df.deputy.str.lower().str.strip().fillna("").str.extract(r"(\w+) ?(\w+)? (\w+)")
    )

    df.loc[:, "first_name"] = names[0].fillna("")
    df.loc[:, "middle_name"] = names[1].fillna("")
    df.loc[:, "last_name"] = names[2].fillna("")
    return df[~((df.first_name == "") & (df.last_name == ""))].drop(columns="deputy")


def clean_allegation(df):
    df.loc[:, "allegation"] = (
        df.allegation.str.lower()
        .str.strip()
        .str.replace(r"\bia\b", "internal affairs", regex=True)
    )
    return df


def clean_action(df):
    df.loc[:, "action"] = (
        df.action.str.lower()
        .str.strip()
        .str.replace(r"^(\w{1}) day", r"\1-day", regex=True)
        .str.replace(r"^resigned/terminated$", "terminated; resigned", regex=True)
    )
    return df


def clean():
    df = (
        pd.read_csv(data_file_path("raw/acadia_so/cprr_acadia_so_2018_2021.csv"))
        .pipe(clean_column_names)
        .rename(columns={"date": "receive_date"})
        .pipe(clean_dates, ["receive_date"])
        .pipe(extract_names)
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(clean_allegation)
        .pipe(clean_action)
        .pipe(set_values, {"agency": "Acadia SO"})
        .pipe(gen_uid, ["agency", "first_name", "middle_name", "last_name"])
        .pipe(
            gen_uid,
            ["uid", "allegation", "action", "receive_day", "receive_month"],
            "allegation_uid",
        )
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(data_file_path("clean/cprr_acadia_so_2018_2021.csv"), index=False)
