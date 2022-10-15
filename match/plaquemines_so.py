from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex
import deba
from lib.post import extract_events_from_post, load_for_agency
import pandas as pd
from lib.clean import canonicalize_officers


def deduplicate_pprr(pprr):
    df = pprr[["uid", "first_name", "last_name"]]
    df = df.drop_duplicates(subset=["uid"]).set_index("uid")
    df.loc[:, "fc"] = df.first_name.fillna("").map(lambda x: x[:1])
    matcher = ThresholdMatcher(
        ColumnsIndex("fc"),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        df,
    )
    decision = 0.950
    matcher.save_clusters_to_excel(
        deba.data("match/deduplicate_pprr_plaquemines_2018.xlsx"),
        decision,
        decision,
    )
    clusters = matcher.get_index_clusters_within_thresholds(decision)
    return canonicalize_officers(pprr, clusters)


def match_cprr_19_and_pprr(cprr, pprr):
    dfa = (
        cprr[["uid", "first_name", "last_name"]]
        .drop_duplicates(subset=["uid"])
        .set_index("uid", drop=True)
    )
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = (
        pprr[["uid", "first_name", "last_name"]]
        .drop_duplicates(subset=["uid"])
        .set_index("uid", drop=True)
    )
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {"first_name": JaroWinklerSimilarity(), "last_name": JaroWinklerSimilarity()},
        dfa,
        dfb,
    )
    decision = 0.97
    matcher.save_pairs_to_excel(
        deba.data("match/plaquemines_so_cprr_2019_v__pprr_plaquemines_so_2018.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_pprr_and_post(pprr, post):
    dfa = pprr[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = post[["uid", "first_name", "last_name"]]
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
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
    decision = 0.93
    matcher.save_pairs_to_excel(
        deba.data("match/plaquemines_so_pprr_2018_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    return extract_events_from_post(post, matches, "plaquemines-so")


def match_cprr_2016_2020_and_pprr(cprr, pprr):
    dfa = cprr[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = pprr[["uid", "first_name", "last_name"]]
    dfb.loc[:, "fc"] = dfb.first_name.map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.94
    matches = matcher.save_pairs_to_excel(
        deba.data("match/cprr_plaquemines_so_2016_2019_v_pprr_plaqumines_so_2018.xlsx"),
        decision,
    )

    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


if __name__ == "__main__":
    cprr19 = pd.read_csv(deba.data("clean/cprr_plaquemines_so_2019.csv"))
    cprr20 = pd.read_csv(deba.data("clean/cprr_plaquemines_so_2016_2020.csv"))
    pprr = pd.read_csv(deba.data("clean/pprr_plaquemines_so_2018.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    pprr = deduplicate_pprr(pprr)
    cprr19 = match_cprr_19_and_pprr(cprr19, pprr)
    cprr20 = match_cprr_2016_2020_and_pprr(cprr20, pprr)
    post_event = match_pprr_and_post(pprr, post)
    cprr19.to_csv(deba.data("match/cprr_plaquemines_so_2019.csv"), index=False)
    cprr20.to_csv(deba.data("match/cprr_plaquemines_so_2016_2020.csv"), index=False)
    post_event.to_csv(deba.data("match/event_plaquemines_so_2018.csv"), index=False)
    pprr.to_csv(deba.data("match/pprr_plaquemines_so_2018.csv"), index=False)
