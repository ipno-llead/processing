import pandas as pd
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex

import deba
from lib.post import (
    extract_events_from_post,
    extract_events_from_cprr_post,
    load_for_agency,
)


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
        deba.data("match/ponchatoula_pd_pprr_2010_2020_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    return extract_events_from_post(post, matches, "ponchatoula-pd")


def match_cprr_and_pprr(cprr, pprr):
    dfa = cprr[["first_name", "last_name", "uid"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates().set_index("uid", drop=True)

    dfb = pprr[["last_name", "first_name", "uid"]]
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
        deba.data("match/ponchatoula_pd_cprr_2010_2020_v_pprr_2010_2020.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def extract_cprr_post_events(pprr, cprr_post):
    dfa = pprr[["first_name", "last_name", "uid", "agency"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"], ignore_index=True).set_index(
        "uid", drop=True
    )

    dfb = cprr_post[["first_name", "last_name", "uid", "agency"]]
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"], ignore_index=True).set_index(
        "uid", drop=True
    )

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc", "agency"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.95
    matcher.save_pairs_to_excel(
        deba.data("match/pprr_ponchatoula_pd_2010_2020_v_cprr_post_2016_2019.csv.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    return extract_events_from_cprr_post(cprr_post, matches, "ponchatoula-pd")


if __name__ == "__main__":
    pprr = pd.read_csv(deba.data("clean/pprr_ponchatoula_pd_2010_2020.csv"))
    cprr = pd.read_csv(deba.data("clean/cprr_ponchatoula_pd_2010_2020.csv"))
    agency = cprr.agency[0]
    post = load_for_agency(agency)
    cprr_post = pd.read_csv(deba.data("match/cprr_post_2016_2019.csv"))
    cprr = match_cprr_and_pprr(cprr, pprr)
    post_events = extract_post_events(pprr, post)
    cprr_post_events = extract_cprr_post_events(pprr, cprr_post)
    post_events.to_csv(
        deba.data("match/post_event_ponchatoula_pd_2020.csv"), index=False
    )
    cprr.to_csv(deba.data("match/cprr_ponchatoula_pd_2010_2020.csv"), index=False)
    cprr_post_events.to_csv(
        deba.data("match/cprr_post_events_ponchatoula_pd_2020.csv"), index=False
    )
