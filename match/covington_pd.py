import pandas as pd
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex

import deba
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
        deba.data("match/covington_pd_ah_2021_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    return extract_events_from_post(post, matches, "covington-pd")


def extract_post_events(pprr_25, post):
    dfa = pprr_25.set_index("uid", drop=True)
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
    decision = 0.96
    matcher.save_pairs_to_excel(
        deba.data("match/covington_pd_pprr_1975_2025_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    return extract_events_from_post(post, matches, "covington-pd")


def match_uof_against_pprr(uof, pprr):
    dfa = uof[["uid", "first_name", "last_name"]]
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid", drop=True)

    dfb = pprr[["uid", "first_name", "last_name"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid", drop=True)

    matcher = ThresholdMatcher(
        ColumnsIndex([]),
        {
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.9
    matcher.save_pairs_to_excel(
        deba.data("match/covington_pd_uof_2022_2025_v_pprr_covington_pd_1975_2025.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)
    uof.loc[:, "uid"] = uof.uid.map(lambda x: match_dict.get(x, x))
    return uof


if __name__ == "__main__":
    ah = pd.read_csv(deba.data("clean/actions_history_covington_pd_2021.csv"))
    pprr = pd.read_csv(deba.data("clean/pprr_covington_pd_2020.csv"))
    pprr_25 = pd.read_csv(deba.data("clean/pprr_covington_pd_1975_2025.csv"))
    uof = pd.read_csv(deba.data("clean/uof_covington_pd_2022_2025.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    uof = match_uof_against_pprr(uof, pprr_25)
    post_events = extract_post_events(
        pd.concat(
            [
                ah[["first_name", "last_name", "uid"]],
                pprr[["first_name", "last_name", "uid"]],
                pprr_25[["first_name", "last_name", "uid"]],
            ]
        ).drop_duplicates(),
        post,
    )
    post_events.to_csv(deba.data("match/post_event_covington_pd_2020.csv"), index=False)
    pprr_25.to_csv(deba.data("match/pprr_covington_pd_1975_2025.csv"), index=False)
    uof.to_csv(deba.data("match/uof_covington_pd_2022_2025.csv"), index=False)

