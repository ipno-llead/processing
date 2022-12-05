import pandas as pd
from datamatch import ThresholdMatcher, ColumnsIndex
import re
from lib.rows import duplicate_row
from lib.columns import rearrange_coaccusal_columns
import deba


def create_clusters(df):
    df = (
        df.loc[df.tracking_id.notna(), ["uid", "tracking_id"]]
        .groupby("tracking_id")
        .filter(lambda x: len(x) >= 2)
    )
    df = pd.DataFrame(
        ((tn, set(sr.tolist())) for tn, sr in df.groupby(["tracking_id"])["uid"]),
        columns=["tracking_id", "uids"],
    ).set_index("tracking_id", drop=True)

    def scorer(a: pd.Series, b: pd.Series) -> float:
        x_len = len(a.uids & b.uids)
        if x_len < 2:
            return 0
        return x_len * 2 / (len(a.uids) + len(b.uids))

    matcher = ThresholdMatcher(
        index=ColumnsIndex("uids", index_elements=True),
        scorer=scorer,
        dfa=df,
        show_progress=True,
    )
    decision = 0.1
    matcher.save_clusters_to_excel(
        deba.data("analysis/allegation.xlsx"), decision, decision
    )

    return matcher.get_clusters_within_threshold(decision)


def split_rows_with_multiple_uids(df):
    df.loc[:, "uids"] = df.uids.str.replace(r"({|}|\')", "", regex=True)
    i = 0
    for idx in df[df.uids.str.contains(",")].index:
        s = df.loc[idx + i, "uids"]
        parts = re.split(r"\s*(?:\,)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "uids"] = name
        i += len(parts) - 1
    return df.rename(columns={"uids": "uid"})


def reformat_clusters(df):
    clusters = (
        pd.read_excel(deba.data("analysis/allegation.xlsx"))
        .drop(columns=["cluster_idx", "pair_idx", "sim_score"])
        .rename(columns={"row_key": "tracking_id"})
    )
    clusters = clusters.pipe(split_rows_with_multiple_uids)

    df = df[["allegation_uid", "tracking_id", "uid", "agency"]]

    clusters = pd.merge(clusters, df, on=["uid", "tracking_id"])
    clusters = clusters.drop_duplicates(subset=["allegation_uid"])
    return df[~((df.tracking_id.fillna("") == ""))]


if __name__ == "__main__":
    df = pd.read_csv(deba.data("fuse/allegation.csv"))
    gen_clusters = create_clusters(df)
    clusters = reformat_clusters(df)
    clusters = rearrange_coaccusal_columns(clusters)
    clusters.to_csv(deba.data("analysis/coaccusals.csv"), index=False)
