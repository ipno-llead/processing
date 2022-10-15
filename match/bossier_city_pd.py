from datamatch import JaroWinklerSimilarity, ThresholdMatcher, ColumnsIndex
import deba
from lib.post import extract_events_from_post, load_for_agency
from lib.clean import canonicalize_officers
import pandas as pd


def deduplicate_cprr20(cprr):
    df = cprr[["uid", "first_name", "last_name"]]
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
        deba.data("match/dedeuplicate_cprr_bossier_city_pd_2020.xlsx"),
        decision,
        decision,
    )
    clusters = matcher.get_index_clusters_within_thresholds(decision)
    return canonicalize_officers(cprr, clusters)


def extract_post_events(pprr, post):
    dfa = pprr[["first_name", "last_name", "uid"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = post[["first_name", "last_name", "uid"]]
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
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
    decision = 0.95
    matcher.save_pairs_to_excel(
        deba.data("match/pprr_bossier_city_pd_v_post_2016_2019.xlsx"), decision
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    return extract_events_from_post(post, matches, "bossier-city-pd")


def match_cprr_with_pprr(cprr, pprr):
    dfa = cprr[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
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
    decision = 0.81
    matcher.save_pairs_to_excel(
        deba.data(
            "match/cprr_bossier_city_pd_2020_2014_v_pprr_bossier_city_pd_2009_2020.xlsx"
        ),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr["uid"].map(lambda v: match_dict.get(v, v))
    return cprr


if __name__ == "__main__":
    cprr = pd.read_csv(deba.data("clean/cprr_bossier_city_pd_2020.csv"))
    pprr = pd.read_csv(deba.data("clean/pprr_bossier_city_pd_2000_2019.csv"))
    agency = cprr.agency[0]
    post = load_for_agency(agency)
    cprr = deduplicate_cprr20(cprr)
    cprr = match_cprr_with_pprr(cprr, pprr)
    post_event = extract_post_events(pprr, post)
    post_event.to_csv(deba.data("match/post_event_bossier_city_pd.csv"), index=False)
    cprr.to_csv(deba.data("match/cprr_bossier_city_pd_2020.csv"), index=False)
