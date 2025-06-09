import deba
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.clean import (
    clean_names,
    clean_dates,
    names_to_title_case,
    standardize_desc_cols,
)
from lib.uid import gen_uid
from functools import reduce


def split_rows_with_multiple_allegations(df):
    df = (
        df.drop("allegation", axis=1)
        .join(
            df["allegation"]
            .str.split(";", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("allegation"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    return df


def clean_action(df):
    df.loc[:, "action"] = df.action.str.replace(
        r"suspensino", "suspension", regex=False
    )
    return df


def clean():
    df = (
        pd.read_csv(
            deba.data("raw/louisiana_state_pc/lspc_letters_2015_2019.csv")
        )
        .drop(columns=["filename", "officer_middle_name"])
        .pipe(clean_column_names)
        .rename(columns={"officer_first_name": "first_name",
                         "officer_last_name": "last_name",
                        "department_description": "department_desc"})
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(standardize_desc_cols, ["rank", "allegation", "department_desc", "action", "disposition"])
        .pipe(clean_dates, ["incident_date", "action_date"])
        # .pipe(split_rows_with_multiple_allegations)
        # .pipe(clean_action)
        .pipe(set_values, {"agency": "louisiana-state-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["allegation", "action", "uid"], "allegation_uid")
        # .drop_duplicates(subset=["allegation_uid"])
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_louisiana_state_pc_2015_2019.csv"), index=False)
