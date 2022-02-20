import pandas as pd
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex

import bolo
from lib.post import extract_events_from_post, load_for_agency


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
        bolo.data("match/sterlington_pd_pprr_2010_2020_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    return extract_events_from_post(post, matches, "Sterlington PD")


if __name__ == "__main__":
    pprr = pd.read_csv(bolo.data("clean/pprr_sterlington_csd_2010_2020.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    post_event = match_pprr_and_post(pprr, post)
    post_event.to_csv(
        bolo.data("match/post_event_sterlington_pd_2010_2020.csv"), index=False
    )
