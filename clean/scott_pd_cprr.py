from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid
import bolo
from lib.clean import clean_names, standardize_desc_cols, clean_dates
import pandas as pd


def split_names_20(df):
    names = df.full_name.str.split(" ", expand=True)
    df.loc[:, "rank_desc"] = names.loc[:, 0].fillna("")
    df.loc[:, "first_name"] = names.loc[:, 1].fillna("")
    df.loc[:, "last_name"] = names.loc[:, 2].fillna("")
    df = df.drop(columns="full_name")
    return df


def split_names_14(df):
    df.loc[:, "officer"] = df.officer.fillna("")
    names = df.officer.str.extract(r"(\w+) (\w+) (\w+)")
    df.loc[:, "rank_desc"] = names[0]
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "last_name"] = names[2]
    return df.dropna().drop(columns="officer")


def split_disposition_action_20(df):
    outcomes = df["disposition_action"].str.split("/", expand=True)
    df.loc[:, "disposition"] = outcomes.loc[:, 0].fillna("")
    df.loc[:, "action"] = outcomes.loc[:, 1].fillna("")
    df = df.drop(columns="disposition_action")
    return df


def split_disposition_action_14(df):
    df.loc[:, "outcome_final_disposition"] = (
        df.outcome_final_disposition.str.strip()
        .str.lower()
        .str.replace("arrest/ termination", "arrested; terminated", regex=False)
    )

    outcomes = df.outcome_final_disposition.str.split("/", expand=True)

    df.loc[:, "disposition"] = outcomes.loc[:, 0].fillna("")
    df.loc[:, "action"] = (
        outcomes.loc[:, 1].fillna("").str.replace("no action", "", regex=False)
    )
    return df.drop(columns="outcome_final_disposition")


def clean_rank(df):
    df.loc[:, "rank_desc"] = (
        df.rank_desc.str.lower()
        .str.strip()
        .str.replace("pofc", "officer", regex=False)
        .str.replace("lt.", "lieutenant ", regex=False)
    )
    return df


def clean_allegations(df):
    df.loc[:, "allegation"] = (
        df.rule_violation.str.lower()
        .str.strip()
        .str.replace(
            "conduct unbecoming/ rude behavior",
            "conduct unbecoming (rude behavior)",
            regex=False,
        )
        .str.replace(
            "conduct unbecoming/lying", "conduct unbecoming (lying)", regex=False
        )
        .str.replace(
            "conduct unbecoming/rudeness", "conduct unbecoming (rudeness)", regex=False
        )
    )
    df = df.drop(columns="rule_violation")
    return df


def clean20():
    df = (
        pd.read_csv(bolo.data("raw/scott_pd/scott_pd_cprr_2020.csv"))
        .pipe(clean_column_names)
        .drop(columns="appeal")
        .dropna()
        .pipe(split_names_20)
        .pipe(standardize_desc_cols, ["rank_desc", "disposition_action"])
        .pipe(clean_allegations)
        .pipe(clean_rank)
        .pipe(set_values, {"agency": "Scott PD"})
        .pipe(split_disposition_action_20)
        .pipe(clean_dates, ["notification_date"])
        .rename(
            columns={
                "notification_year": "receive_year",
                "notification_month": "receive_month",
                "notification_day": "receive_day",
            }
        )
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
        .pipe(
            gen_uid,
            ["agency", "uid", "receive_year", "receive_month", "receive_day"],
            "allegation_uid",
        )
    )
    return df


def clean14():
    df = (
        pd.read_csv(bolo.data("raw/scott_pd/scott_pd_cprr_2009_2014.csv"))
        .pipe(clean_column_names)
        .drop(columns=["appeal"])
        .rename(columns={"date": "receive_date", "offense": "allegation"})
        .pipe(split_names_14)
        .pipe(clean_rank)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(split_disposition_action_14)
        .pipe(standardize_desc_cols, ["allegation"])
        .pipe(clean_dates, ["receive_date"])
        .pipe(set_values, {"agency": "Scott PD"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["uid", "allegation", "disposition", "action"], "allegation_uid")
    )
    return df


if __name__ == "__main__":
    df20 = clean20()
    df14 = clean14()
    df20.to_csv(bolo.data("clean/cprr_scott_pd_2020.csv"), index=False)
    df14.to_csv(bolo.data("clean/cprr_scott_pd_2009_2014.csv"), index=False)
