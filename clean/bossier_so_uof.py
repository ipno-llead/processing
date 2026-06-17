import pandas as pd
import deba
from lib.clean import (
    clean_dates,
    clean_names,
    standardize_desc_cols,
)
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid


def strip_leading_apostrophes(df):
    for col in df.columns:
        df[col] = df[col].str.replace(r"^\'", "", regex=True)
    return df


def parse_officer_name(df):
    parts = df.off_name.str.extract(
        r"^([^,]+),\s*(\S+)\s+([A-Za-z]\.?)\s*(?:\((\d+)\))?\s*$"
    )
    no_middle = df.off_name.str.extract(
        r"^([^,]+),\s*(\S+)\s*(?:\((\d+)\))?\s*$"
    )
    df.loc[:, "last_name"] = parts[0].fillna(no_middle[0]).str.strip()
    df.loc[:, "first_name"] = parts[1].fillna(no_middle[1]).str.strip()
    df.loc[:, "middle_name"] = parts[2].str.strip().str.replace(r"\.$", "", regex=True)
    df.loc[:, "badge_no"] = parts[3].fillna(no_middle[2])
    return df.drop(columns=["off_name"])


def format_occurred_time(df):
    time = df.hour_occu.str.strip().str.zfill(4)
    df.loc[:, "occurred_time"] = time.str[:2] + ":" + time.str[2:]
    return df.drop(columns=["hour_occu"])


def clean():
    df = (
        pd.read_csv(
            deba.data("raw/bossier_so/bossier_so_uof_23_26.csv")
        )
        .pipe(clean_column_names)
        .pipe(strip_leading_apostrophes)
        .rename(columns={
            "date_occu": "occurred_date",
            "officer_id": "officer_id_og",
        })
        .pipe(parse_officer_name)
        .pipe(format_occurred_time)
        .pipe(clean_dates, ["occurred_date"])
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(
            standardize_desc_cols,
            ["badge_no", "officer_id_og"],
        )
        .pipe(set_values, {"agency": "bossier-so"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            [
                "uid",
                "occurred_year",
                "occurred_month",
                "occurred_day",
                "occurred_time",
            ],
            "uof_uid",
        )
    )

    uof_df = df[
        [
            "occurred_year",
            "occurred_month",
            "occurred_day",
            "occurred_time",
            "badge_no",
            "officer_id_og",
            "first_name",
            "middle_name",
            "last_name",
            "agency",
            "uid",
            "uof_uid",
        ]
    ]

    return uof_df


if __name__ == "__main__":
    uof = clean()
    uof.to_csv(deba.data("clean/uof_bossier_so_2023_2026.csv"), index=False)
