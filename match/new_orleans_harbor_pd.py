from datamatch import ThresholdMatcher, ColumnsIndex, JaroWinklerSimilarity
import pandas as pd

import deba
from lib.uid import gen_uid
from lib.post import extract_events_from_post, load_for_agency


def match_uid_with_cprr(cprr, pprr):
    """
    match cprr and pprr records to add "uid" column to cprr
    """
    # generate column "mid" to merge
    cprr = gen_uid(cprr, ["first_name", "last_name"], "mid")
    cprr = cprr.drop_duplicates(subset=["mid"])

    # limit number of columns before matching
    dfa = cprr[["mid", "first_name", "last_name"]].drop_duplicates()
    dfa.loc[:, "fc"] = dfa.first_name.map(lambda x: x[:1])
    dfa = dfa.set_index("mid", drop=True)
    dfb = pprr[["uid", "first_name", "last_name"]].drop_duplicates()
    dfb.loc[:, "fc"] = dfb.first_name.map(lambda x: x[:1])
    dfb = dfb.set_index("uid", drop=True)
    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.96
    matcher.save_pairs_to_excel(
        deba.data("match/new_orleans_harbor_pd_cprr_2020_v_pprr_2020.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)

    mid_to_uid_d = dict(matches)
    cprr.loc[:, "uid"] = cprr["mid"].map(lambda v: mid_to_uid_d[v])
    cprr = cprr.drop(columns=["mid"])
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
    decision = 0.96
    matcher.save_pairs_to_excel(
        deba.data("match/new_orleans_harbor_pd_pprr_2020_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    return extract_events_from_post(post, matches, "new-orleans-harbor-pd")

def match_pprr25_and_post(pprr25, post):
    dfa = pprr25[["uid", "first_name", "last_name"]]
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
    decision = 0.98
    matcher.save_pairs_to_excel(
        deba.data("match/new_orleans_harbor_pd_pprr_2025_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    return extract_events_from_post(post, matches, "new-orleans-harbor-pd")


if __name__ == "__main__":
    cprr = pd.read_csv(deba.data("clean/cprr_new_orleans_harbor_pd_2020.csv"))
    pprr20 = pd.read_csv(deba.data("clean/pprr_new_orleans_harbor_pd_2020.csv"))
    pprr25 = pd.read_csv(deba.data("clean/pprr_new_orleans_harbor_pd_1990_2025.csv"))
    agency = pprr20.agency[0]
    post = load_for_agency(agency)
    cprr = match_uid_with_cprr(cprr, pprr20)
    post_event = match_pprr_and_post(pprr20, post)
    post_event2 = match_pprr25_and_post(pprr25, post)
    post_event = pd.concat([post_event, post_event2], ignore_index=True)
    cprr.to_csv(deba.data("match/cprr_new_orleans_harbor_pd_2020.csv"), index=False)
    post_event.to_csv(
        deba.data("match/post_event_new_orleans_harbor_pd_2020.csv"), index=False
    )
    pprr25.to_csv(deba.data("match/pprr_new_orleans_harbor_pd_1990_2025.csv"), index=False)
