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


def experiment(post):
    for col in post.columns:
        if col == "uid":
            post = match_uid(post, personnel)
            post.rename(columns={"matched_uid": "matched_uid_0"}, inplace=True)
        elif col == "uid_1":
            post = match_uid(post, personnel)
            post.rename(columns={"matched_uid": "matched_uid_1"}, inplace=True)
        elif col == "uid_2":
            post.rename(columns={"uid_2": "uid"})
            post = match_uid(post, personnel)
            post.rename(columns={"matched_uid": "matched_uid_2"}, inplace=True)
        elif col == "uid_3":
            post.rename(columns={"uid_3": "uid"})
            post = match_uid(post, personnel)
            post.rename(columns={"matched_uid": "matched_uid_3"}, inplace=True)
        elif col == "uid_4":
            post.rename(columns={"uid_4": "uid"})
            post = match_uid(post, personnel)
            post.rename(columns={"matched_uid": "matched_uid_4"}, inplace=True)
        elif col == "uid_5":
            post.rename(columns={"uid_5": "uid"})
            post = match_uid(post, personnel)
            post.rename(columns={"matched_uid": "matched_uid_5"}, inplace=True)
        elif col == "uid_6":
            post.rename(columns={"uid_6": "uid"})
            post = match_uid(post, personnel)
            post.rename(columns={"matched_uid": "matched_uid_6"}, inplace=True)
        elif col == "uid_7":
            post.rename(columns={"uid_7": "uid"})
            post = match_uid(post, personnel)
            post.rename(columns={"matched_uid": "matched_uid_7"}, inplace=True)
        elif col == "uid_8":
            post.rename(columns={"uid_8": "uid"})
            post = match_uid(post, personnel)
            post.rename(columns={"matched_uid": "matched_uid_8"}, inplace=True)
    return post


def match_post_to_personnel(post):
    for col in post.columns:
        if col == "uid":
            post = match_uid(post, personnel)
            post.rename(columns={"matched_uid": "matched_uid_0"}, inplace=True)
        if col == "uid_1":
            post.rename(columns={"uid_1": "uid"}, inplace=True)
            post = match_uid(post, personnel)
            post.rename(columns={"matched_uid": "matched_uid_1"}, inplace=True)
        if col == "uid_2":
            post.rename(columns={"uid_2": "uid"}, inplace=True)
            post = match_uid(post, personnel)
            post.rename(columns={"matched_uid": "matched_uid_2"}, inplace=True)
        if col == "uid_3":
            post.rename(columns={"uid_3": "uid"}, inplace=True)
            post = match_uid(post, personnel)
            post.rename(columns={"matched_uid": "matched_uid_3"}, inplace=True)
        if col == "uid_4":
            post.rename(columns={"uid_4": "uid"}, inplace=True)
            post = match_uid(post, personnel)
            post.rename(columns={"matched_uid": "matched_uid_4"}, inplace=True)
        if col == "uid_5":
            post.rename(columns={"uid_5": "uid"}, inplace=True)
            post = match_uid(post, personnel)
            post.rename(columns={"matched_uid": "matched_uid_5"}, inplace=True)
        if col == "uid_6":
            post.rename(columns={"uid_6": "uid"}, inplace=True)
            post = match_uid(post, personnel)
            post.rename(columns={"matched_uid": "matched_uid_6"}, inplace=True)
        if col == "uid_7":
            post.rename(columns={"uid_7": "uid"}, inplace=True)
            post = match_uid(post, personnel)
            post.rename(columns={"matched_uid": "matched_uid_7"}, inplace=True)
        if col == "uid_8":
            post.rename(columns={"uid_8": "uid"}, inplace=True)
            post = match_uid(post, personnel)
            post.rename(columns={"matched_uid": "matched_uid_8"}, inplace=True)
    return post


def join_matched_uids(df):
    df.loc[:, "post_id"] = df[
        [col for col in df.columns if col.startswith("matched_uid_")]
    ].apply(lambda sr: ",".join(sr[sr.notna()].to_list()))
    df = df[["post_id"]]
    return df


if __name__ == "__main__":
    post = pd.read_csv(deba.data("fuse/cleaned_post_officer_history.csv"))
    personnel = pd.read_csv(deba.data("fuse/events_and_personnel_merged.csv"))
    post = experiment(post, personnel)
    post.to_csv(deba.data("fuse/matched_post_officer_history.csv"), index=False)
