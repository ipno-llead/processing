import sys

import pandas as pd
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex

from lib.path import data_file_path
from lib.post import extract_events_from_post

sys.path.append("../")


def extract_post_events(pprr, post):
    dfa = pprr[["first_name", "last_name", "uid"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates().set_index("uid", drop=True)

    dfb = post[["last_name", "first_name", "uid"]]
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid", drop=True)

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.95
    matcher.save_pairs_to_excel(
        data_file_path("match/slidell_csd_pprr_2010_2019_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    return extract_events_from_post(post, matches, "Slidell PD")


if __name__ == "__main__":
    pprr_csd = pd.read_csv(data_file_path("clean/pprr_slidell_csd_2010_2019.csv"))
    agency = pprr_csd.agency[0]
    post = pd.read_csv(data_file_path("clean/pprr_post_2020_11_06.csv"))
    post = post.loc[post.agency == agency]
    post_events = extract_post_events(pprr_csd, post)
    post_events.to_csv(data_file_path("match/post_event_slidell_pd_2020.csv"), index=False)
