import pandas as pd
import deba
from datamatch import JaroWinklerSimilarity, ThresholdMatcher, ColumnsIndex
from lib.post import load_for_agency
from lib.clean import canonicalize_officers


def deduplicate_cprr20(cprr):
    df = cprr[["uid", "first_name", "last_name", "middle_name"]]
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
    decision = 0.955
    matcher.save_clusters_to_excel(
        deba.data("match/deduplicate_cprr_lafayette_so_20.xlsx"),
        decision,
        decision,
    )
    clusters = matcher.get_index_clusters_within_thresholds(decision)
    return canonicalize_officers(cprr, clusters)


def deduplicate_cprr14(cprr):
    df = cprr[["uid", "first_name", "last_name", "middle_name"]]
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
    decision = 0.955
    matcher.save_clusters_to_excel(
        deba.data("match/deduplicate_cprr_lafayette_so_14.xlsx"),
        decision,
        decision,
    )
    clusters = matcher.get_index_clusters_within_thresholds(decision)
    return canonicalize_officers(cprr, clusters)


def deduplicate_cprr08(cprr):
    df = cprr[["uid", "first_name", "last_name", "middle_name"]]
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
    decision = 0.955
    matcher.save_clusters_to_excel(
        deba.data("match/deduplicate_cprr_lafayette_so_08.xlsx"),
        decision,
        decision,
    )
    clusters = matcher.get_index_clusters_within_thresholds(decision)
    return canonicalize_officers(cprr, clusters)


def match_cprr_20_and_post(cprr, post):
    dfa = cprr[["uid", "first_name", "last_name"]]
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = post[["uid", "first_name", "last_name"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex("fc"),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.8
    matcher.save_pairs_to_excel(
        deba.data("match/lafayette_so_2015_2020_vs_post_2020_11_06.xlsx"), decision
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_cprr_14_and_post(cprr, post):
    dfa = cprr[["uid", "first_name", "last_name"]]
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = post[["uid", "first_name", "last_name"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex("fc"),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.95
    matcher.save_pairs_to_excel(
        deba.data("match/lafayette_so_2009_2014_vs_post_2020_11_06.xlsx"), decision
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_cprr_08_with_post(cprr, post):
    dfa = cprr[["uid", "first_name", "last_name"]]
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = post[["uid", "first_name", "last_name"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex("fc"),
        {"first_name": JaroWinklerSimilarity(), "last_name": JaroWinklerSimilarity()},
        dfa,
        dfb,
    )
    decision = 0.936

    matcher.save_pairs_to_excel(
        deba.data("match/cprr_lafayette_so_2006_2008_v_post_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_award_17_with_post(award, post):
    dfa = award[["uid", "first_name", "last_name"]]
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = post[["uid", "first_name", "last_name"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex("fc"),
        {"first_name": JaroWinklerSimilarity(), "last_name": JaroWinklerSimilarity()},
        dfa,
        dfb,
    )
    decision = 0.943

    matcher.save_pairs_to_excel(
        deba.data("match/award_lafayette_so_2017_v_post_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    award.loc[:, "uid"] = award.uid.map(lambda x: match_dict.get(x, x))
    return award


if __name__ == "__main__":
    cprr20 = pd.read_csv(deba.data("clean/cprr_lafayette_so_2015_2020.csv"))
    cprr14 = pd.read_csv(deba.data("clean/cprr_lafayette_so_2009_2014.csv"))
    cprr08 = pd.read_csv(deba.data("clean/cprr_lafayette_so_2006_2008.csv"))
    award17 = pd.read_csv(deba.data("clean/award_lafayette_so_2017.csv"))
    agency = cprr08.agency[0]
    post = load_for_agency(agency)
    cprr20 = deduplicate_cprr20(cprr20)
    cprr14 = deduplicate_cprr14(cprr14)
    cprr08 = deduplicate_cprr08(cprr08)
    cprr20 = match_cprr_20_and_post(cprr20, post)
    cprr14 = match_cprr_14_and_post(cprr14, post)
    cprr08 = match_cprr_08_with_post(cprr08, post)
    award17 = match_award_17_with_post(award17, post)
    cprr20.to_csv(deba.data("match/cprr_lafayette_so_2015_2020.csv"), index=False)
    cprr14.to_csv(deba.data("match/cprr_lafayette_so_2009_2014.csv"), index=False)
    cprr08.to_csv(deba.data("match/cprr_lafayette_so_2006_2008.csv"), index=False)
    award17.to_csv(deba.data("match/award_lafayette_so_2017.csv"), index=False)
