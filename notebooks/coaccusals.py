import pandas as pd
from pandas_dedupe import dedupe_dataframe

def convert_to_str(df):
    return df[df.allegation_uid.astype(str)]

def filter_out_non_duplicates(df: pd.DataFrame):
    return df.loc[df.tracking_id.notna(), ['uid', 'tracking_id', "allegation_uid"]].groupby("tracking_id").filter(lambda x: len(x) >= 2)


def groupby_tracking_number_and_uid(df):
    return pd.DataFrame(
        ((tn, id.str.cat(sep=", ")) for tn, id in df.groupby(['tracking_id', "allegation_uid"])['uid']),
        columns=["ids", "uids"])


def split_cols(df):
    df.loc[:, "ids"] = df.ids.astype(str).str.replace(r"(\(|\)|\')", "", regex=True)
    df[["tracking_id", "allegation_uid"]] = df['ids'].str.split(r'\,', expand=True)
    return df.drop(columns=["ids"])


def create_clusters(df):
    df = dedupe_dataframe(df, ['uids'])
    return df

"""
3     243e11a7128df89c570314aed55a9dc8  2020-0565-r  2ca005708d4aa4ec982294f764cb71d8
4     14b8b33c301f5580041a965e27d1c2e6  2020-0565-r  4713804b02e3e76dfbca8da1c2dfe710
5     14b8b33c301f5580041a965e27d1c2e6  2020-0565-r  3557a6f8b03ebb802e4ea1d5463b1504
"""


def filter_clusters_for_exact_coaccusals(df):
    df = df[df.duplicated(subset=['cluster_id'], keep=False)].sort_values(by=["cluster_id"])
    return df


def cluster():
    df = pd.read_csv("data/clean/cprr_new_orleans_da_2016_2020.csv")
    df = (df
        .pipe(filter_out_non_duplicates)
        .pipe(groupby_tracking_number_and_uid)
        .pipe(split_cols)
        # .pipe(create_clusters)
        # .rename(columns={"cluster id": "cluster_id"})
        # .pipe(filter_clusters_for_exact_coaccusals)
    )
    return df