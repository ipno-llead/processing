import sys

sys.path.append("../")
import pandas as pd
import deba
from lib.clean import (
    clean_sexes,
    standardize_desc_cols,
    clean_dates,
    clean_races,
    clean_names,
)
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid
from lib.rows import duplicate_row
import re


def split_rows_with_multiple_officers(df):
    i = 0
    for idx in df[df.officer.str.contains("/")].index:
        s = df.loc[idx + i, "officer"]
        parts = re.split(r"\s*(?:\/)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "officer"] = name
        i += len(parts) - 1
    return df


def split_officer_names(df):
    names = (
        df.officer.str.lower()
        .str.strip()
        .str.replace(r"^lpcc$", "", regex=True)
        .str.replace(r"lt\.", "lieutenant", regex=True)
        .str.replace(r"(\w+) $", "", regex=True)
        .str.extract(r"(lieutenant)? ?(\w+) ?(\w{1})? (.+)")
    )

    df.loc[:, "rank_desc"] = names[0]
    df.loc[:, "first_name"] = names[1].fillna("")
    df.loc[:, "middle_name"] = names[2]
    df.loc[:, "last_name"] = names[3].fillna("")
    return df.drop(columns=["officer"])


def drop_rows_missing_names(df):
    return df[~((df.first_name == "") & (df.last_name == ""))]


def clean_receive_date(df):
    df.loc[:, "receive_date"] = (
        df.date.astype(str)
        .str.lower()
        .str.strip()
        .str.replace(r"^carry over for 2020$", "", regex=True)
    )
    return df.drop(columns=["date"])


def extract_race_and_sex_columns(df):
    races = df.race_sex_office.str.lower().str.strip().str.extract(r"(^b|^w)")
    df.loc[:, "race"] = (
        races[0]
        .fillna("")
        .str.replace(r"^b$", "black", regex=True)
        .str.replace(r"^w$", "white", regex=True)
    )

    sexes = df.race_sex_office.str.lower().str.strip().str.extract(r"(f$|m$)")

    df.loc[:, "sex"] = (
        sexes[0]
        .fillna("")
        .str.replace(r"^f$", "female", regex=True)
        .str.replace(r"^m$", "male", regex=True)
    )
    return df.drop(columns=["race_sex_office"])


def extract_disposition(df):
    dispositions = (
        df.outcome.str.lower()
        .str.strip()
        .str.replace(r"(\w+) $", r"\1", regex=True)
        .str.extract(r"(\bnot sustained\b|sustained|unfounded|\bno violations\b)")
    )

    df.loc[:, "disposition"] = dispositions[0]
    return df


def clean_actions(df):
    df.loc[:, "action"] = (
        df.outcome.str.lower()
        .str.strip()
        .str.replace(r"(\w+) $", r"\1", regex=True)
        .str.replace(r"\.", "", regex=True)
        .str.replace(r"\bterminated\b", "termination", regex=True)
        .str.replace(r"\b40 hrs of susp\b", "40-hour suspension", regex=True)
        .str.replace(r" ?\/ ?", " ", regex=True)
        .str.replace(r"^resignation in lieu$", "resignation", regex=True)
        .str.replace(r"transfer 8 hrs", "8-hour transfer", regex=True)
        .str.replace(
            r"^resigned under investigation",
            "resignation while under investigation",
            regex=True,
        )
        .str.replace("refered", "referred", regex=False)
        .str.replace(
            r"(^abandoned$|^handeled at the division$|no violations|^unfounded$|\bnot sustained\b|\bsustained\b)",
            "",
            regex=True,
        )
    )
    return df


def clean_allegation(df):
    df.loc[:, "allegation"] = (
        df.allegation.str.lower()
        .str.strip()
        .str.replace(r"art\.?", "article", regex=True)
        .str.replace(r"^terminated$", "", regex=True)
        .str.replace(r"takeing\b", "taking", regex=True)
        .str.replace(r"\bherrasment\b", "harrassment", regex=True)
        .str.replace(r"^no disciplin$", "", regex=True)
        .str.replace(r"^po ", "", regex=True)
        .str.replace(r"^(\w{2})\b", r"article \1", regex=True)
        .str.replace(r"\/", ",", regex=True)
        .str.replace(r"(\w{2})\, (\w{2})", r"\1,\2", regex=True)
        .str.replace(r"^fratinization$", "fraternization", regex=True)
    )
    return df.drop(columns=["outcome"])


def clean():
    df = (
        pd.read_csv(deba.data("raw/lafourche_so/lafourche_so_cprr_2019_2021.csv"))
        .pipe(clean_column_names)
        .rename(columns={"case": "tracking_id"})
        .drop(columns=["complainant", "race_sex"])
        .pipe(split_rows_with_multiple_officers)
        .pipe(split_officer_names)
        .pipe(drop_rows_missing_names)
        .pipe(clean_receive_date)
        .pipe(clean_dates, ["receive_date"])
        .pipe(extract_race_and_sex_columns)
        .pipe(extract_disposition)
        .pipe(clean_actions)
        .pipe(clean_allegation)
        .pipe(standardize_desc_cols, ["action", "tracking_id", "allegation"])
        .pipe(clean_races, ["race"])
        .pipe(clean_sexes, ["sex"])
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(set_values, {"agency": "Lafourche SO"})
        .pipe(gen_uid, ["first_name", "middle_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["uid", "tracking_id", "allegation", "disposition", "action"],
            "allegation_uid",
        )
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_lafourche_so_2019_2021.csv"), index=False)
