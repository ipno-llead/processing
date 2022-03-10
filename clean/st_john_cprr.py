import sys

sys.path.append("../")
import pandas as pd
import deba
from lib.columns import clean_column_names, set_values
from lib.clean import clean_names, standardize_desc_cols
from lib.rows import duplicate_row
from lib.uid import gen_uid
import re


def concat_and_split_rows_with_multiple_officers(df):
    df.loc[:, "officer_name"] = df.last_first_name.fillna("").str.cat(
        df.last_first_name_1.fillna(""), sep="/"
    )
    i = 0
    for idx in df[df.officer_name.str.contains("/")].index:
        s = df.loc[idx + i, "officer_name"]
        parts = re.split(r"\s*(?:\/)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "officer_name"] = name
        i += len(parts) - 1
    return df.drop(columns=["last_first_name", "last_first_name_1"])


def split_officer_names(df):
    names = (
        df.officer_name.str.lower()
        .str.strip()
        .str.replace(r"  +", " ", regex=True)
        .str.replace(r"(\w+) $", r"\1", regex=True)
        .str.extract(r"(\w+), (\w+) ?(.+)?")
    )

    df.loc[:, "last_name"] = names[0].fillna("")
    df.loc[:, "first_name"] = names[1].fillna("")
    df.loc[:, "suffix"] = names[2].fillna("")

    df.loc[:, "last_name"] = df.last_name.str.cat(df.suffix, sep=" ")
    return df.drop(columns=["suffix", "officer_name"])


def drop_rows_missing_names(df):
    return df[~((df.first_name == "") & (df.last_name == ""))]


def clean():
    df = (
        pd.read_csv(deba.data("raw/st_john_so/st_john_so_cprr_2018_2020.csv"))
        .pipe(clean_column_names)
        .drop(columns=["personnel_free_form", "cfs"])
        .rename(columns={"complaint_number": "tracking_number"})
        .pipe(concat_and_split_rows_with_multiple_officers)
        .pipe(split_officer_names)
        .pipe(standardize_desc_cols, ["tracking_number"])
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(drop_rows_missing_names)
        .pipe(set_values, {"agency": "St. John SO"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["uid", "tracking_number", "case_number"], "allegation_uid")
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_st_john_so_2018_2020.csv"), index=False)
