import pandas as pd
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex

import deba
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
        deba.data("match/west_monroe_pd_pprr_2015_2020_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    return extract_events_from_post(post, matches, "west-monroe-pd")


def match_cprr_with_pprr(cprr, pprr):
    dfa = (
        cprr[["first_name", "last_name", "uid"]]
        .drop_duplicates(subset=["uid"])
        .set_index("uid", drop=True)
    )
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfb = (
        pprr[["first_name", "last_name", "uid"]]
        .drop_duplicates(subset=["uid"])
        .set_index("uid", drop=True)
    )
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.82
    matcher.save_pairs_to_excel(
        deba.data(
            "match/cprr_west_monroe_pd_2020_v_pprr_west_monroe_pd_2015_2020.xlsx"
        ),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)

    match_dict = dict(matches)
    cprr.loc[:, "uid"] = cprr["uid"].map(lambda v: match_dict.get(v, v))
    return cprr


if __name__ == "__main__":
    pprr = pd.read_csv(deba.data("clean/pprr_west_monroe_pd_2015_2020.csv"))
    cprr = pd.read_csv(deba.data("clean/cprr_west_monroe_pd_2020.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    post_event = match_pprr_and_post(pprr, post)
    match_cprr_with_pprr(cprr, pprr)
    post_event.to_csv(
        deba.data("match/post_event_west_monroe_pd_2020_11_06.csv"), index=False
    )
