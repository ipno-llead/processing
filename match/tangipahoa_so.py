import pandas as pd
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex
import deba
from lib.post import load_for_agency
from lib.clean import canonicalize_officers


def deduplicate_cprr_officers(cprr):
    df = cprr[["uid", "first_name", "last_name", "rank_desc"]]
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
    decision = 0.866
    matcher.save_clusters_to_excel(
        deba.data("match/deduplicate_tangipahoa_so_cprr_2015_2021.xlsx"),
        decision,
        decision,
    )
    clusters = matcher.get_index_clusters_within_thresholds(decision)
    return canonicalize_officers(cprr, clusters)


def match_cprr21_post(cprr, post):
    dfa = cprr[["uid", "first_name", "last_name"]]
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = post[["uid", "first_name", "last_name"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex("fc"),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.873
    matcher.save_pairs_to_excel(
        deba.data("match/tangipahoa_so_cprr_2015_2021_v_pprr_post_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_cprr13_post(cprr, post):
    dfa = cprr[["uid", "first_name", "last_name"]]
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = post[["uid", "first_name", "last_name"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex("fc"),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.873
    matcher.save_pairs_to_excel(
        deba.data("match/tangipahoa_so_cprr_2013_v_pprr_post_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


if __name__ == "__main__":
    cprr21 = pd.read_csv(deba.data("clean/cprr_tangipahoa_so_2015_2021.csv"))
    cprr13 = pd.read_csv(deba.data("clean/tangipahoa_so_cprr_2013.csv"))
    agency = cprr21.agency[0]
    post = load_for_agency(agency)
    cprr21 = deduplicate_cprr_officers(cprr21)
    cprr21 = match_cprr21_post(cprr21, post)
    cprr13 = match_cprr13_post(cprr13, post)
    cprr21.to_csv(deba.data("match/cprr_tangipahoa_so_2015_2021.csv"), index=False)
    cprr13.to_csv(deba.data("match/tangipahoa_so_cprr_2013.csv"), index=False)
