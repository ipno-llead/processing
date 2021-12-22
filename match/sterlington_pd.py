import sys

import pandas as pd
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex

from lib.path import data_file_path
from lib.post import extract_events_from_post

sys.path.append("../")


def prepare_post_data():
    post = pd.read_csv(data_file_path("clean/pprr_post_2020_11_06.csv"))
    return post[post.agency == "sterlington pd"]


def match_pprr_and_post(pprr, post):
    dfa = pprr[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = post[["uid", "first_name", "last_name"]]
    dfb.loc[:, "fc"] = dfb.first_name.map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "last_name": JaroWinklerSimilarity(),
            "first_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.9
    matcher.save_pairs_to_excel(
        data_file_path(
            "match/sterlington_pd_pprr_2010_2020_v_post_pprr_2020_11_06.xlsx"
        ),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    return extract_events_from_post(post, matches, "Sterlington PD")


if __name__ == "__main__":
    pprr = pd.read_csv(data_file_path("clean/pprr_sterlington_csd_2010_2020.csv"))
    post = prepare_post_data()
    post_event = match_pprr_and_post(pprr, post)
    post_event.to_csv(
        data_file_path("match/post_event_sterlington_pd_2010_2020.csv"), index=False
    )
