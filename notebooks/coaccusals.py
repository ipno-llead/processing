import pandas as pd
from pandas_dedupe import dedupe_dataframe
import re
from lib.rows import duplicate_row


def filter_out_non_duplicates(df: pd.DataFrame):
    df = df.loc[df.tracking_id.notna(), ['uid', 'tracking_id']].groupby("tracking_id").filter(lambda x: len(x) >= 2)
    df["uid_counts"] = df.loc[df.tracking_id.notna(), ['uid', 'tracking_id']].groupby("tracking_id")["uid"].transform("nunique")
    df = df[~((df.uid_counts.astype(str) == "1"))]
    return df.drop(columns=["uid_counts"])


def groupby_tracking_number_and_uid(df):
    return pd.DataFrame(
        ((tn, id.str.cat(sep=", ")) for tn, id in df.groupby(['tracking_id'])['uid']),
        columns=["tracking_id", "uids"])


def create_clusters(df):
    df = dedupe_dataframe(df, ["uids"])
    return df

"""
3     243e11a7128df89c570314aed55a9dc8  2020-0565-r  2ca005708d4aa4ec982294f764cb71d8
4     14b8b33c301f5580041a965e27d1c2e6  2020-0565-r  4713804b02e3e76dfbca8da1c2dfe710
5     14b8b33c301f5580041a965e27d1c2e6  2020-0565-r  3557a6f8b03ebb802e4ea1d5463b1504
"""


def filter_clusters_for_exact_coaccusals(df):
    df = df[df.duplicated(subset=['cluster_id'], keep=False)]
    return df


def split_rows_with_multiple_uids(df):
    df = (
        df.drop("uids", axis=1)
        .join(
            df["uids"]
            .str.split(", ", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("uids"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    return df.rename(columns={"uids": "uid"})


def cluster():
    dfa = pd.read_csv("data/clean/cprr_new_orleans_da_2016_2020.csv")
    dfb = (dfa
        .pipe(filter_out_non_duplicates)
        .pipe(groupby_tracking_number_and_uid)
        .pipe(create_clusters)
        .rename(columns={"cluster id": "cluster_id"})
        .pipe(filter_clusters_for_exact_coaccusals)
        .pipe(split_rows_with_multiple_uids)
    )
    dfa = dfa[["allegation_uid", "tracking_id", "uid"]]
    df = pd.merge(dfa, dfb, on=["uid", "tracking_id"])
    df = df.drop_duplicates(subset=["allegation_uid"])   
    return df.sort_values("cluster_id").drop(columns=["confidence"])