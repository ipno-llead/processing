import pandas as pd
import deba
from lib.clean import standardize_desc_cols


def drop_missing_agency(df):
    return df[~((df.agency.fillna("") == ""))]


def drop_missing_names(df):
    return df[~((df.first_name.fillna("") == "") & (df.last_name.fillna("") == ""))]


def drop_missing_matched_uid(df):
    return df[~(df.matched_uid.fillna("") == "")]


def uids(df):
    df.loc[:, "uids"] = df.matched_uid.fillna("").str.cat(
        df.matched_uid_1.fillna(""), sep=","
    )
    df.loc[:, "uids"] = df.uids.str.cat(df.matched_uid_2.fillna(""), sep=",")
    df.loc[:, "uids"] = df.uids.str.cat(df.matched_uid_3.fillna(""), sep=",")
    df.loc[:, "uids"] = df.uids.str.cat(df.matched_uid_4.fillna(""), sep=",")
    df.loc[:, "uids"] = df.uids.str.cat(df.matched_uid_5.fillna(""), sep=",")
    df.loc[:, "uids"] = df.uids.str.cat(df.matched_uid_6.fillna(""), sep=",")
    df.loc[:, "uids"] = df.uids.str.cat(df.matched_uid_7.fillna(""), sep=",")
    df.loc[:, "uids"] = df.uids.str.cat(df.matched_uid_8.fillna(""), sep=",")

    df.loc[:, "uids"] = df.uids.str.replace(r"\,\, ?(.+)?", "", regex=True)
    return df.fillna("")


def clean():
    df = (
        pd.read_csv(deba.data("raw/post/post_officer_history.csv"))
        .pipe(drop_missing_agency)
        .pipe(drop_missing_names)
        .pipe(drop_missing_matched_uid)
        .pipe(uids)
        .pipe(standardize_desc_cols, ["uids"])
    )

    df = df[["uids"]]
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/post_officer_history.csv"), index=False)
