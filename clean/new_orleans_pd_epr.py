import deba
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.clean import (
    clean_races,
    clean_sexes,
    float_to_int_str,
    standardize_desc_cols,
    clean_dates,
    clean_names,
)
from lib.uid import gen_uid


def split_rows_with_multiple_officers(df):
    df = (
        df.drop("officer_name", axis=1)
        .join(
            df["officer_name"]
            .str.split("/", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("officer_name"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    return df


def split_names(df):
    names1 = (
        df.officer_name.str.lower()
        .str.strip()
        .str.replace(r"(\w+) (\w) (\w+)", r"\1 \2. \3", regex=True)
        .str.replace(r" de los angeles", "delosangeles", regex=False)
        .str.replace(r"(\w+) +(\w+)", r"\1 \2", regex=True)
        .str.replace(r"\,(\w+)\.$", r" \1", regex=True)
        .str.replace(r"(\w+)\.(\w+)", r"\1. \2", regex=True)
        .str.extract(r"^(\w+-?\w+?) ?(?:(\w)?\.? ) ?(.+)")
    )

    df.loc[:, "first_name"] = names1[0]
    df.loc[:, "middle_name"] = names1[1].fillna("")
    df.loc[:, "last_name"] = names1[2].str.replace(r"\,", "", regex=True)

    return df[~((df.last_name.fillna("") == ""))].drop(columns=["officer_name"])


def drop_rows_missing_names(df):
    return df[~((df.last_name.fillna("") == ""))]


def split_date_col(df):
    col = df.occurred_date_time.str.replace(
        r"(\w+)-(\w+)-(\w+)", r"\2/\3/\1", regex=True
    ).str.extract(r"(\w+\/\w+\/\w+) (.+)")

    df.loc[:, "occurred_date"] = col[0]
    df.loc[:, "occurred_time"] = col[1]
    return df.drop(columns=["occurred_date_time"])


def filter_arrested(df):
    df = df[df.offender_status.isin(["ARRESTED"])]
    return df


def join_names(df):
    cols = ["first_name", "last_name"]
    df.loc[:, "name"] = df.first_name.fillna("").str.cat(df.last_name, sep=" ")
    return df


def clean():

    dfa = (
        pd.read_csv(
            deba.data("raw/new_orleans_pd/new_orleans_pd_epr_2010_2022.csv"),
            encoding="utf-8",
        )
        .astype(str)
        .drop(columns=["persontype", "hate_crime"])
        .rename(
            columns={
                "offender_gender": "offender_sex",
                "victim_gender": "victim_sex",
                "offenderstatus": "offender_status",
                "offenderid": "offender_id",
            }
        )
        .pipe(split_rows_with_multiple_officers)
        .pipe(split_names)
        .pipe(
            standardize_desc_cols,
            [
                "disposition",
                "signal_description",
                "charge_description",
                "victim_fatal_status",
            ],
        )
        .pipe(split_date_col)
        .pipe(clean_races, ["offender_race", "victim_race"])
        .pipe(clean_sexes, ["offender_sex", "victim_sex"])
        .pipe(
            float_to_int_str,
            ["offender_number", "offender_age", "victim_age", "offender_id"],
        )
        .pipe(set_values, {"agency": "new-orleans-pd"})
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            [
                "uid",
                "item_number",
                "disposition",
                "charge_description",
                "occurred_date",
            ],
            "police_report_uid",
        )
    )

    dfb = (
        pd.read_csv(
            deba.data("raw/new_orleans_pd/rtcc_footage_request_id.csv"),
            encoding="ISO-8859-1",
        )
        .astype(str)
        .pipe(clean_column_names)
        .pipe(standardize_desc_cols, ["item_number"])
        .pipe(set_values, {"rtcc_footage_requested": "yes"})
    )

    df = pd.merge(dfa, dfb, on="item_number", how="outer")

    df = df.pipe(drop_rows_missing_names).drop_duplicates(
        subset=["uid", "police_report_uid"], keep="last"
    )

    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/pr_new_orleans_pd_2010_2022.csv"), index=False)
