import pandas as pd
import deba
from lib.clean import (
    clean_dates,
    clean_names,
    clean_races,
    clean_sexes,
    standardize_desc_cols,
)
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid


def split_datetime_col(df):
    data = df.incident_start_date_time.str.extract(
        r"(\d{1,2}\/\d{1,2}\/\d{2,4})\b ?(.+)?"
    )
    df.loc[:, "occurred_date"] = data[0]
    df.loc[:, "occurred_time"] = data[1].str.strip().str.zfill(5)
    return df.drop(columns=["incident_start_date_time"])


def split_officer_name(df):
    parts = df.last_first_name.str.extract(r"^([^,]+),\s*(.+)$")
    df.loc[:, "last_name"] = parts[0].str.strip()
    df.loc[:, "first_name"] = parts[1].str.strip()
    return df.drop(columns=["last_first_name"])


def clean_disposition(df):
    df.loc[:, "disposition"] = (
        df.disposition.str.lower().str.strip()
    )
    return df


def clean_call_type(df):
    df.loc[:, "call_type"] = (
        df.description.str.lower()
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )
    return df.drop(columns=["description"])


def create_tracking_id_og_col(df):
    df.loc[:, "tracking_id_og"] = df.tracking_id
    return df


def clean():
    df = (
        pd.read_csv(
            deba.data("raw/assumption_so/assumption_so_uof_23_26.csv")
        )
        .pipe(clean_column_names)
        .rename(
            columns={
                "case_number": "tracking_id",
                "address": "incident_location",
                "race": "officer_race",
                "sex": "officer_sex",
                "age": "officer_age",
            }
        )
    )
    df = df[df.tracking_id.str.contains(r"^[A-Za-z]", na=False)].reset_index(
        drop=True
    )
    df = (
        df.pipe(split_datetime_col)
        .pipe(split_officer_name)
        .pipe(clean_call_type)
        .pipe(clean_disposition)
        .pipe(clean_dates, ["occurred_date"])
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(clean_races, ["officer_race"])
        .pipe(clean_sexes, ["officer_sex"])
        .pipe(
            standardize_desc_cols,
            ["incident_location", "tracking_id", "disposition"],
        )
        .pipe(set_values, {"agency": "assumption-so"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            [
                "uid",
                "tracking_id",
                "call_type",
                "occurred_year",
                "occurred_month",
                "occurred_day",
            ],
            "uof_uid",
        )
        .pipe(create_tracking_id_og_col)
        .pipe(gen_uid, ["tracking_id", "agency"], "tracking_id")
    )

    uof_df = df[
        [
            "tracking_id",
            "tracking_id_og",
            "occurred_year",
            "occurred_month",
            "occurred_day",
            "occurred_time",
            "call_type",
            "incident_location",
            "disposition",
            "officer_race",
            "officer_sex",
            "officer_age",
            "first_name",
            "last_name",
            "agency",
            "uid",
            "uof_uid",
        ]
    ]

    return uof_df


if __name__ == "__main__":
    uof = clean()
    uof.to_csv(deba.data("clean/uof_assumption_so_2022_2026.csv"), index=False)
