import sys

sys.path.append("../")
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.path import data_file_path
from lib.clean import clean_dates, clean_names
from lib.uid import gen_uid


def split_rows_with_multiple_officers(df):
    df.loc[:, "focus_officer"] = (
        df.focus_officer.str.lower()
        .str.strip()
        .str.replace(r"^1\-", "", regex=True)
        .str.replace(r"(2\-?|3\-?|4\-?)", "/", regex=True)
    )
    df = (
        df.drop("focus_officer", axis=1)
        .join(
            df["focus_officer"]
            .str.split("/", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("focus_officer"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    return df


def split_officer_names(df):
    names = df.focus_officer.str.extract(r"(ptl|pco|sgt|pfc|ptl)\. (\w+) (\w+)")
    df.loc[:, "rank_desc"] = names[0]
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "last_name"] = names[2]
    return df.drop(columns="focus_officer")


def clean_allegation(df):
    df.loc[:, "allegation"] = (
        df.complaint.str.lower()
        .str.strip()
        .str.replace(r"\-", "", regex=True)
        .str.replace(r"officer (.+)", r"officer; \1", regex=True)
    )
    return df.drop(columns="complaint")


def split_investiator_names(df):
    names = (
        df.assigned_investigator.str.lower()
        .str.strip()
        .str.extract(r"(captain) (\w+) (\w+)")
    )

    df.loc[:, "investigator_rank_desc"] = names[0]
    df.loc[:, "investigator_first_name"] = names[1]
    df.loc[:, "investigator_last_name"] = names[2]
    return df.drop(columns="assigned_investigator")


def clean():
    df = (
        pd.read_csv(data_file_path("raw/rayne_pd/rayne_pd_cprr_2019_2020.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "date_received": "receive_date",
                "date_completed": "investigation_complete_date",
            }
        )
        .pipe(clean_allegation)
        .pipe(split_rows_with_multiple_officers)
        .pipe(split_officer_names)
        .pipe(split_investiator_names)
        .pipe(clean_dates, ["receive_date", "investigation_complete_date"])
        .pipe(set_values, {"agency": "Rayne PD"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
    )
    return df


if __name__ == '__main__':
    df = clean()
    df.to_csv(data_file_path('clean/cprr_rayne_pd_2019_2020.csv'), index=False)
