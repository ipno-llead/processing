import pandas as pd
import deba
from lib.clean import (
    names_to_title_case,
    clean_names,
    clean_post_agency_column,
    clean_sexes,
    clean_agency_row,
)
from lib.uid import gen_uid


def split_names(df):
    names = (
        df.officer_name.str.lower()
        .str.strip()
        .str.extract(r"(\w+(?:'\w+)?),? (\w+)(?: (\w+))?")
    )

    df.loc[:, "last_name"] = names[0]
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "middle_name"] = names[2]
    return df.drop(columns=["officer_name"])


def extract_agency(df):
    for col in df.columns:
        if col.startswith("agency"):
            df[col] = df[col].str.replace(
                r"(\w{1}? ?\w+ ?\w+? ?\w+) (.+)", r"\1", regex=True
            )
    return df


def drop_rows_missing_agency(df):
    return df[~((df.agency.fillna("") == ""))]


def drop_rows_missing_names(df):
    return df[~((df.first_name == "") & (df.last_name == ""))]


def clean():
    df = (
        pd.read_csv(deba.data("raw/post/post_officer_history.csv"))
        .rename(columns={"officer_sex": "sex"})
        .pipe(clean_sexes, ["sex"])
        .pipe(split_names)
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(
            clean_agency_row,
            [
                "agency",
                "agency_1",
                "agency_2",
                "agency_3",
                "agency_4",
                "agency_5",
                "agency_6",
                "agency_7",
                "agency_8",
            ],
        )
        .pipe(extract_agency)
        .pipe(
            names_to_title_case,
            [
                "agency",
                "agency_1",
                "agency_2",
                "agency_3",
                "agency_4",
                "agency_5",
                "agency_6",
                "agency_7",
                "agency_8",
            ],
        )
        .pipe(
            clean_post_agency_column,
            [
                "agency",
                "agency_1",
                "agency_2",
                "agency_3",
                "agency_4",
                "agency_5",
                "agency_6",
                "agency_7",
                "agency_8",
            ],
        )
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency"])
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency_1"], "uid_1")
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency_2"], "uid_2")
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency_3"], "uid_3")
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency_4"], "uid_4")
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency_5"], "uid_5")
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency_6"], "uid_6")
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency_7"], "uid_7")
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency_8"], "uid_8")
        .pipe(drop_rows_missing_agency)
        .pipe(drop_rows_missing_names)
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/post_officer_history.csv"), index=False)
