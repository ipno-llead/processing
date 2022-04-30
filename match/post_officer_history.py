import deba
import pandas as pd
from datamatch import (
    JaroWinklerSimilarity,
    ThresholdMatcher,
    ColumnsIndex,
)


def match_post_to_personnel(post, personnel):
    dfa = post[["uid", "first_name", "last_name", "middle_name", "agency"]]
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = personnel[["uid", "first_name", "last_name", "middle_name", "agency"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc", "agency"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
            "middle_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.90
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    post.loc[:, "uid"] = post.uid.map(lambda x: match_dict.get(x))
    return post


if __name__ == "__main__":
    post = pd.read_csv(deba.data("clean/post_officer_history.csv"))
    personnel = pd.read_csv(deba.data("fuse/events_and_personnel.csv"))
    post = match_post_to_personnel(post, personnel)
    post = post[["uid", "history_id", "false_neg_id"]]
    post.to_csv(deba.data("match/post_officer_history.csv"), index=False)
