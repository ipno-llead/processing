import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid
from lib.clean import clean_dates, clean_races, clean_sexes, standardize_desc_cols
import deba
from lib.rows import duplicate_row
import re


def split_names(df):
    names = df.officer.str.lower().str.strip().str.extract(r"(deputy)? ?(\w+)?\.? (.+)")
    df.loc[:, "rank_desc"] = names[0]
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "last_name"] = names[2]
    return df.drop(columns=["officer"])


def split_reporting_names(df):
    names = df.reporting_officer.str.lower().str.strip().str.extract(r"(\w+) (\w+)")
    df.loc[:, "reporting_officer_first_name"] = names[0]
    df.loc[:, "reporting_officer_last_name"] = names[1]
    return df.drop(columns=["reporting_officer"])


def split_arresting_nammes(df):
    names = (
        df.arresting_officer.str.lower()
        .str.strip()
        .str.extract(r"(capt|lt)?\.? ?(?:(\w+) )? ?(\w+)")
    )
    df.loc[:, ["arresting_officer_rank_desc"]] = names[0]
    df.loc[:, "arresting_officer_first_name"] = names[1]
    df.loc[:, "arresting_officer_last_name"] = names[2]
    return df.drop(columns=["arresting_officer"])


def split_rows_with_multiple_allegations(df):
    df.loc[:, "allegation"] = (
        df.allegation.fillna("")
        .str.lower()
        .str.strip()
        .str.replace(r"\n", "", regex=True)
        .str.replace(r"; (\w+)", r"/\1", regex=True)
        .str.replace(r"(\w+)\. (\w) ?", "", regex=True)
    )

    i = 0
    for idx in df[df.allegation.str.contains("\/")].index:
        s = df.loc[idx + i, "allegation"]
        parts = re.split(r"\s*(?:\/)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "allegation"] = name
        i += len(parts) - 1
    return df


def clean():
    df = (
        pd.read_csv(
            deba.data("raw/morehouse_so/morehouse_so_cprr_2018_2021.csv"),
            encoding="ISO-8859-1",
        )
        .pipe(clean_column_names)
        .drop(columns=["reporting_date", "receive_date"])
        .rename(columns={"receive_date_1": "receive_date"})
        .pipe(split_names)
        .pipe(split_reporting_names)
        .pipe(split_arresting_nammes)
        .pipe(clean_races, ["race"])
        .pipe(clean_sexes, ["sex"])
        .pipe(split_rows_with_multiple_allegations)
        .pipe(clean_dates, ["receive_date", "incident_date", "arrest_date"])
        .pipe(standardize_desc_cols, ["allegation_desc"])
        .pipe(set_values, {"agency": "morehouse-so"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["reporting_officer_first_name", "reporting_officer_last_name", "agency"],
            "reporting_officer_uid",
        )
        .pipe(
            gen_uid,
            ["arresting_officer_first_name", "arresting_officer_last_name", "agency"],
            "arresting_officer_uid",
        )
        .pipe(
            gen_uid,
            [
                "uid",
                "allegation_desc",
                "allegation",
                "arrest_year",
                "arrest_month",
                "arrest_day",
            ],
            "allegation_uid",
        )
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_morehouse_so_2018_2021.csv"), index=False)
