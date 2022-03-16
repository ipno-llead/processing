import deba
import pandas as pd
from lib.columns import clean_column_names
from lib.uid import gen_uid
from lib.columns import set_values


def split_names(df):
    names = df.officer_name.str.lower().str.strip().str.extract(r"(\w+)\. (\w+) (\w+)")

    df.loc[:, "rank_desc"] = (
        names[0]
        .str.replace(r"\.", "", regex=True)
        .str.replace("ofc", "officer", regex=False)
        .str.replace("det", "detective", regex=False)
        .str.replace("lt", "lieutenant", regex=False)
        .str.replace("sgt", "sergeant", regex=False)
    )
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "last_name"] = names[2]
    return df.drop(columns="officer_name")


def join_disposition_columns(df):
    df.loc[:, "disposition"] = (
        df.sustained.str.lower()
        .str.strip()
        .fillna("")
        .str.cat(df.not_sustained.str.lower().str.strip().fillna(""))
    )

    df.loc[:, "disposition"] = (
        df.disposition.str.replace(
            r"resigned his employment prior to investigation being performed\.",
            "resigned",
            regex=True,
        )
        .str.replace("yes", "sustained", regex=False)
        .str.replace("no", "not sustained", regex=False)
    )
    return df.drop(columns=["sustained", "not_sustained"])


def clean_allegation(df):
    df.loc[:, "allegation"] = (
        df.allegations_complaints.str.lower()
        .str.strip()
        .str.replace(r"\.", "", regex=True)
        .str.replace(r"\bdept\b", "department", regex=True)
        .str.replace(r"\badmin\b", "administration", regex=True)
        .str.replace(
            "cooperation with other police agencies - (failure to)",
            "failure to cooperate with other police agencies",
            regex=False,
        )
        .str.replace("zcooperation", "cooperation", regex=False)
        .str.replace(
            "presenting statement of fact - (falsified)",
            "false presentation of statment of fact",
            regex=False,
        )
        .str.replace(
            "use of force - (excessive)", "excessive use of force", regex=False
        )
    )
    return df.drop(columns="allegations_complaints")


def clean():
    df = (
        pd.read_csv(deba.data("raw/baker_pd/baker_pd_cprr_2018_2020.csv"))
        .pipe(clean_column_names)
        .pipe(clean_allegation)
        .pipe(join_disposition_columns)
        .pipe(split_names)
        .pipe(set_values, {"agency": "Baker PD"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["uid", "disposition", "allegation"], "allegation_uid")
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_baker_pd_2018_2020.csv"), index=False)
