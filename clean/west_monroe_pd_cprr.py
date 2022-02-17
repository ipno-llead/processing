import pandas as pd
import dirk
from lib.columns import clean_column_names, set_values
from lib.clean import clean_names, clean_dates, standardize_desc_cols
from lib.uid import gen_uid


def extract_accused_badge_number(df):
    badges = df.name.astype(str).str.extract(r"\((\w+)\)")
    df.loc[:, "badge_number"] = badges[0]
    return df


def split_accused_names(df):
    names = df.name.str.extract(r"^(\w+)\, (\w+)")
    df.loc[:, "last_name"] = names[0]
    df.loc[:, "first_name"] = names[1]
    return df.drop(columns="name")


def extract_receiver_badge_number(df):
    badges = df.received_by.astype(str).str.extract(r"\((\w+)\)")
    df.loc[:, "receiver_badge_number"] = badges[0]
    return df


def split_receiver_names(df):
    names = df.received_by.str.extract(r"(captain)? ?(\w+)\, (\w+)")
    df.loc[:, "receiver_rank_desc"] = names[0]
    df.loc[:, "receiver_last_name"] = names[1]
    df.loc[:, "receiver_first_name"] = names[2]
    return df.drop(columns="received_by")


def fix_review_date(df):
    df.loc[:, "review_date"] = (
        df.review_date.astype(str)
        .str.replace(r"\; (.+)", "", regex=True)
        .str.replace("nan", "", regex=False)
    )
    return df


def clean():
    df = (
        pd.read_csv(dirk.data("raw/west_monroe_pd/cprr_west_monroe_pd_2020_byhand.csv"))
        .drop(columns="allegation_desc")
        .rename(
            columns={
                "investigator_reviewer": "investigator",
                "incident_date": "occur_date",
            }
        )
        .pipe(clean_column_names)
        .pipe(fix_review_date)
        .pipe(clean_dates, ["occur_date", "receive_date", "review_date"])
        .pipe(extract_accused_badge_number)
        .pipe(split_accused_names)
        .pipe(extract_receiver_badge_number)
        .pipe(split_receiver_names)
        .pipe(standardize_desc_cols, ["disposition", "action", "investigator"])
        .pipe(
            clean_names,
            ["first_name", "last_name", "receiver_first_name", "receiver_last_name"],
        )
        .pipe(set_values, {"agency": "West Monroe PD"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["uid", "action", "disposition"], "allegation_uid")
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(dirk.data("clean/cprr_west_monroe_pd_2020.csv"), index=False)
