import pandas as pd
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex

import bolo
from lib.post import extract_events_from_post, load_for_agency


def extract_post_events(roster, post):
    dfa = roster.set_index("uid", drop=True)
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

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
    decision = 0.9
    matcher.save_pairs_to_excel(
        bolo.data("match/covington_pd_ah_2021_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    return extract_events_from_post(post, matches, "Covington PD")


if __name__ == "__main__":
    ah = pd.read_csv(bolo.data("clean/actions_history_covington_pd_2021.csv"))
    pprr = pd.read_csv(bolo.data("clean/pprr_covington_pd_2020.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    post_events = extract_post_events(
        pd.concat(
            [
                ah[["first_name", "last_name", "uid"]],
                pprr[["first_name", "last_name", "uid"]],
            ]
        ).drop_duplicates(),
        post,
    )
    post_events.to_csv(bolo.data("match/post_event_covington_pd_2020.csv"), index=False)
