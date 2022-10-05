import pandas as pd
import deba
from lib.columns import clean_column_names, set_values
from lib.clean import clean_names, standardize_desc_cols
from lib.uid import gen_uid
from lib.rows import duplicate_row
import re


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

def correct_action_19(df):
    df.loc[(df.disposition == "terminated") & (df.last_name == "gant"), "action"] = "terminated"
    df.loc[(df.disposition == "terminated; write up attached") & (df.last_name == "raimer"), "action"] = "terminated"
    df.loc[(df.disposition == "terminated; write up attached") & (df.last_name == "guillory"), "action"] = "terminated"
    df.loc[(df.disposition == "terminated; write up attached") & (df.last_name == "nothanel"), "action"] = "terminated"
    df.loc[(df.disposition == "resigned; criminal charges") & (df.last_name == "nunez"), "action"] = "resigned"
    return df

def split_rows_with_multiple_officers_14(df):
    i = 0
    for idx in df[df.deputy.str.contains("/")].index:
        s = df.loc[idx + i, "deputy"]
        parts = re.split(r"\s*(?:\/)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "deputy"] = name
        i += len(parts) - 1
    return df


def split_name_14(df):
    names = df.deputy.str.lower().str.strip().str.extract(r"(\w+) (.+)")

    df.loc[:, "first_name"] = names[0]
    df.loc[:, "last_name"] = names[1].str.replace(r"(\,|\.)", "", regex=True)
    return df.drop(columns="deputy")


def clean_allegation_14(df):
    df.loc[:, "allegation"] = (
        df.complaint.str.lower()
        .str.strip()
        .str.replace(
            r"^conduct/courtesy complaint$", "conduct unbecoming/courtesy", regex=True
        )
    )
    return df.drop(columns=["complaint"])


def clean_20():
    df = (
        pd.read_csv(deba.data("raw/cameron_so/cameron_so_cprr_2020.csv"))
        .pipe(clean_column_names)
        .pipe(split_name)
        .rename(
            columns={
                "complaint": "allegation",
                "outcome_after_investigation": "disposition",
            }
        )
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(set_values, {"agency": "cameron-so"})
        .pipe(standardize_desc_cols, ["allegation", "disposition"])
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
        .pipe(gen_uid, ["uid", "allegation", "disposition"], "allegation_uid")
    )
    return df


def clean_19():
    df = (
        pd.read_csv(deba.data("raw/cameron_so/cameron_so_cprr_2015_2019.csv"))
        .pipe(clean_column_names)
        .pipe(split_name)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(clean_allegation_19)
        .pipe(clean_disposition_19)
        .pipe(set_values, {"agency": "cameron-so"})
        .pipe(standardize_desc_cols, ["allegation", "disposition"])
        .pipe(correct_action_19)
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
        .pipe(gen_uid, ["uid", "allegation", "disposition"], "allegation_uid")
    )
    return df


def clean_14():
    df = (
        pd.read_csv(deba.data("raw/cameron_so/cameron_so_cprr_2013_2014.csv"))
        .pipe(clean_column_names)
        .rename(columns={"outcome_of_investigation": "disposition"})
        .pipe(split_name)
        .pipe(clean_allegation_14)
        .pipe(standardize_desc_cols, ["allegation", "disposition"])
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(set_values, {"agency": "cameron-so"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["uid", "allegation", "disposition"], "allegation_uid")
    )
    return df


if __name__ == "__main__":
    df_20 = clean_20()
    df_19 = clean_19()
    df_14 = clean_14()
    df_20.to_csv(deba.data("clean/cprr_cameron_so_2020.csv"), index=False)
    df_19.to_csv(deba.data("clean/cprr_cameron_so_2015_2019.csv"), index=False)
    df_14.to_csv(deba.data("clean/cprr_cameron_so_2013_2014.csv"), index=False)
