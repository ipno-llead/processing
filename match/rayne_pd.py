import pandas as pd
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex

import bolo
from lib.post import extract_events_from_post, load_for_agency


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
    decision = 0.981
    matcher.save_pairs_to_excel(
        bolo.data("match/pprr_rayne_pd_2010_2020_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    return extract_events_from_post(post, matches, "Rayne PD")


if __name__ == "__main__":
    pprr = pd.read_csv(bolo.data("clean/pprr_rayne_pd_2010_2020.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    post_event = extract_post_events(pprr, post)
    post_event.to_csv(
        bolo.data("match/post_event_rayne_pd_2020_11_06.csv"), index=False
    )
