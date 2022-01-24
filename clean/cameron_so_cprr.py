import sys

sys.path.append("../")
import pandas as pd
from lib.path import data_file_path
from lib.columns import clean_column_names, set_values
from lib.clean import clean_names, standardize_desc_cols
from lib.uid import gen_uid


def split_name(df):
    names = df.deputy.str.lower().str.strip().str.extract(r"(\w+) (.+)")

    df.loc[:, "first_name"] = names[0]
    df.loc[:, "last_name"] = names[1].str.replace(r"(\,|\.)", "", regex=True)
    return df.drop(columns="deputy")


def clean_allegation_19(df):
    df.loc[:, "allegation"] = (
        df.complaint.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("indrouction", "introduction", regex=False)
    )
    return df[~((df.allegation == ""))].drop(columns="complaint")


def clean_disposition_19(df):
    df.loc[:, "disposition"] = (
        df.outcome_after_investigation.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("complaiant", "complainant", regex=False)
        .str.replace("deputy ", "", regex=False)
        .str.replace(r"\,", ";", regex=True)
    )
    return df[~((df.disposition == ""))].drop(columns="outcome_after_investigation")


def clean_20():
    df = (
        pd.read_csv(data_file_path("raw/cameron_so/cameron_so_cprr_2020.csv"))
        .pipe(clean_column_names)
        .pipe(split_name)
        .rename(
            columns={
                "complaint": "allegation",
                "outcome_after_investigation": "disposition",
            }
        )
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(set_values, {"agency": "Cameron SO"})
        .pipe(standardize_desc_cols, ["allegation", "disposition"])
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
        .pipe(gen_uid, ["uid", "allegation", "disposition"], "allegation_uid")
    )
    return df


def clean_19():
    df = (
        pd.read_csv(data_file_path("raw/cameron_so/cameron_so_cprr_2015_2019.csv"))
        .pipe(clean_column_names)
        .pipe(split_name)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(clean_allegation_19)
        .pipe(clean_disposition_19)
        .pipe(set_values, {"agency": "Cameron SO"})
        .pipe(standardize_desc_cols, ["allegation", "disposition"])
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
        .pipe(gen_uid, ["uid", "allegation", "disposition"], "allegation_uid")
    )
    return df


if __name__ == "__main__":
    df_20 = clean_20()
    df_19 = clean_19()
    df_20.to_csv(data_file_path("clean/cprr_cameron_so_2020.csv"), index=False)
    df_19.to_csv(data_file_path("clean/cprr_cameron_so_2015_2019.csv"), index=False)
