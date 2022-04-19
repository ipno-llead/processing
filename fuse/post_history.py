from ntpath import join
import deba
import pandas as pd
from datamatch import (
    JaroWinklerSimilarity,
    ThresholdMatcher,
    ColumnsIndex,
)


def match_uid(post, personnel):
    dfa = post[
        [
            "uid",
            "first_name",
            "last_name",
            "agency",
            "middle_name",
        ]
    ]
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])

    dfb = personnel[["uid", "first_name", "last_name", "agency", "middle_name"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
            "agency": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.9

    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    post.loc[:, "matched_uid"] = post.uid.map(lambda x: match_dict.get(x))
    return post


def join_matched_uids(df):
    df.loc[:, "post_id"] = df[
        [col for col in df.columns if col.startswith("matched_uid_")]
    ].apply(lambda sr: ",".join(sr[sr.notna()].to_list()))
    df = df[["post_id"]]
    return df


if __name__ == "__main__":
    post = pd.read_csv(deba.data("clean/post_officer_history.csv"))
    personnel = pd.read_csv(deba.data("fuse/events_and_personnel_merged.csv"))
    post = match_uid(post, personnel)
    post = join_matched_uids(post)
    post.to_csv(deba.data("fuse/post_officer_history.csv"), index=False)
