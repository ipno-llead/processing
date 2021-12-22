import sys

sys.path.append("../")
from lib.path import data_file_path
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.clean import clean_dates
from lib.uid import gen_uid
import re
from lib.rows import duplicate_row


def clean_tracking_number(df):
    df.loc[:, "tracking_number"] = (
        df.ia_case_number.str.lower()
        .str.strip()
        .str.replace("ia case number", "", regex=False)
    )
    return df.drop(columns="ia_case_number")


def clean_allegation(df):
    df.loc[:, "allegation"] = (
        df.allegation.str.lower()
        .str.strip()
        .str.replace("allegation", "", regex=False)
        .str.replace(
            r"gen\. order 121 - unit usage", "general order 121: unit usage", regex=True
        )
        .str.replace(
            "107 - misuse of equipment",
            "general order 107: misuse of equipment",
            regex=False,
        )
    )
    return df


def clean_disposition(df):
    df.loc[:, "disposition"] = (
        df.disposition.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"non\- ?sustained", "not sustained", regex=True)
        .str.replace("disposition", "", regex=False)
    )
    return df


def clean_action(df):
    df.loc[:, "action"] = (
        df.discipline.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("none", "", regex=False)
        .str.replace("discipline", "", regex=False)
        .str.replace("2 days", "2-day", regex=False)
    )
    return df.drop(columns="discipline")


def clean_allegation_desc(df):
    df.loc[:, "allegation_desc"] = (
        df.notes.str.lower()
        .str.strip()
        .str.replace(r"gen\.", "general", regex=True)
        .str.replace("notes", "", regex=False)
    )
    return df.drop(columns="notes")


def split_rows_with_multiple_officers(df):
    df.loc[:, "officer_name"] = (
        df.officer_name.str.lower().str.strip().str.replace(r" \/ ", "/", regex=True)
    )
    i = 0
    for idx in df[df.officer_name.str.contains("/")].index:
        s = df.loc[idx + i, "officer_name"]
        parts = re.split(r"\s*(?:\/)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "officer_name"] = name
        i += len(parts) - 1
    return df


def split_and_clean_names(df):
    names = df.officer_name.str.replace("officer name", "", regex=False).str.extract(
        r"(\w+) ?(\w+)? (\w+)$"
    )

    df.loc[:, "first_name"] = names[0].fillna("")
    df.loc[:, "middle_name"] = names[1].fillna("")
    df.loc[:, "last_name"] = names[2].fillna("")
    return df.drop(columns="officer_name")


def drop_rows_missing_names(df):
    return df[~((df.first_name == "") & (df.last_name == ""))]


def clean():
    df = (
        pd.read_csv(data_file_path("raw/abbeville_pd/abbeville_pd_cprr_2019_2021.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "date_received": "receive_date",
                "date_completed": "investigation_complete_date",
            }
        )
        .pipe(clean_tracking_number)
        .pipe(clean_allegation)
        .pipe(clean_disposition)
        .pipe(clean_action)
        .pipe(clean_allegation_desc)
        .pipe(split_rows_with_multiple_officers)
        .pipe(split_and_clean_names)
        .pipe(drop_rows_missing_names)
        .pipe(clean_dates, ["receive_date", "investigation_complete_date"])
        .pipe(set_values, {"agency": "Abbeville PD"})
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
        .pipe(
            gen_uid,
            [
                "uid",
                "allegation",
                "disposition",
                "action",
                "receive_year",
                "receive_day",
                "receive_month",
            ],
            "allegation_uid",
        )
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(data_file_path("clean/cprr_abbeville_pd_2019_2021.csv"))
