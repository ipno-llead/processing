from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex
import pandas as pd

import deba
from lib.post import extract_events_from_post, load_for_agency


def match_pprr_post(pprr, post):
    dfa = pprr[["first_name", "last_name", "uid"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates().set_index("uid", drop=True)

    dfb = post[["first_name", "last_name", "uid"]]
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates().set_index("uid", drop=True)

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.9
    matcher.save_pairs_to_excel(
        deba.data("match/grand_isle_pd_pprr_2021_v_post.xlsx"), decision
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    return extract_events_from_post(post, matches, "grand-isle-pd")


if __name__ == "__main__":
    pprr = pd.read_csv(deba.data("clean/pprr_grand_isle_pd_2021.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    post_event = match_pprr_post(pprr, post)

    post_event.to_csv(deba.data("match/post_event_grand_isle_pd.csv"), index=False)
