import pandas as pd
import deba
from lib.clean import (
    clean_dates,
    clean_names,
    standardize_desc_cols,
    strip_leading_comma,
)
from lib.columns import set_values
from lib.uid import gen_uid


def read_and_align_columns():
    df = pd.read_csv(
        deba.data("raw/baker_pd/baker_pd_uof_2021_2025.csv"),
        header=0,
    )
    df = df.iloc[:, :5].fillna("")
    df.columns = ["tracking_id", "date", "call_type", "officer", "incident_location"]
    return df


def fix_merged_address(df):
    mask = df.incident_location.str.strip().eq("")
    merged = df.loc[mask, "officer"].str.extract(
        r"^(.+?\))\s+(.+)$"
    )
    df.loc[mask, "incident_location"] = merged[1]
    df.loc[mask, "officer"] = merged[0]
    return df


def parse_officer(df):
    parts = df["officer"].str.extract(
        r"^([^,]+),\s*([^(]+?)(?:\s*\(([^)]+)\))?\s*$"
    )
    df.loc[:, "last_name"] = parts[0].str.strip()
    df.loc[:, "first_name"] = parts[1].str.strip()
    df.loc[:, "badge_no"] = parts[2]
    return df.drop(columns=["officer"])


def extract_date(df):
    df.loc[:, "occurred_date"] = (
        df["date"].str.strip().str.extract(r"(\d{1,2}/\d{1,2}/\d{4})")[0]
    )
    return df.drop(columns=["date"])


def clean_call_type(df):
    df.loc[:, "call_type"] = (
        df.call_type.str.lower()
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )
    return df


def create_tracking_id_og_col(df):
    df.loc[:, "tracking_id_og"] = df.tracking_id
    return df


def clean():
    df = (
        read_and_align_columns()
        .pipe(strip_leading_comma)
        .pipe(fix_merged_address)
        .pipe(parse_officer)
        .pipe(extract_date)
        .pipe(clean_dates, ["occurred_date"])
        .pipe(clean_call_type)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(
            standardize_desc_cols,
            ["incident_location", "tracking_id", "badge_no"],
        )
        .pipe(set_values, {"agency": "baker-pd"})
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
            "call_type",
            "incident_location",
            "badge_no",
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
    uof.to_csv(deba.data("clean/uof_baker_pd_2021_2025.csv"), index=False)

#forgot to dvc push
