from datamatch import ThresholdMatcher, NoopIndex, JaroWinklerSimilarity, ColumnsIndex
import pandas as pd

import dirk
from lib.post import extract_events_from_post, load_for_agency


def match_cprr_and_pprr(cprr, pprr):
    dfa = cprr[["last_name", "first_name"]]
    dfb = pprr[["last_name", "first_name", "uid"]].drop_duplicates()
    dfb = dfb.set_index("uid", drop=True)
    matcher = ThresholdMatcher(
        NoopIndex(),
        {"first_name": JaroWinklerSimilarity(), "last_name": JaroWinklerSimilarity()},
        dfa,
        dfb,
    )
    decision = 1
    matcher.save_pairs_to_excel(
        dirk.data("match/madisonville_pd_cprr_2010_2020_v_csd_pprr_2019.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    for idx, uid in matches:
        cprr.loc[idx, "uid"] = uid
    cprr = cprr.drop(columns=["first_name", "last_name"])
    return cprr


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
    decision = 0.95
    matcher.save_pairs_to_excel(
        dirk.data("match/madisonville_csd_pprr_2019_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    return extract_events_from_post(post, matches, "Madisonville PD")


if __name__ == "__main__":
    cprr = pd.read_csv(dirk.data("clean/cprr_madisonville_pd_2010_2020.csv"))
    pprr = pd.read_csv(dirk.data("clean/pprr_madisonville_csd_2019.csv"))
    agency = cprr.agency[0]
    post = load_for_agency(agency)
    cprr = match_cprr_and_pprr(cprr, pprr)
    post_event = match_pprr_and_post(pprr, post)

    cprr.to_csv(dirk.data("match/cprr_madisonville_pd_2010_2020.csv"), index=False)
    post_event.to_csv(
        dirk.data("match/post_event_madisonville_csd_2019.csv"), index=False
    )
