import pandas as pd
import deba
from datamatch import JaroWinklerSimilarity, ThresholdMatcher, ColumnsIndex
from lib.post import load_for_agency


def match_cprr21_and_post(cprr, post):
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
    decision = 0.92
    matcher.save_pairs_to_excel(
        deba.data("match/cprr_benton_pd_2015_2021_v_post_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_cprr14_and_post(cprr, post):
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
    decision = .986
    matcher.save_pairs_to_excel(
        deba.data("match/cprr_benton_pd_2009_2014_v_post_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_cprr04_and_post(cprr, post):
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
    decision = 1
    matcher.save_pairs_to_excel(
        deba.data("match/cprr_benton_pd_2004_2008_v_post_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr

if __name__ == "__main__":
    cprr21 = pd.read_csv(deba.data("clean/cprr_benton_pd_2015_2021.csv"))
    cprr14 = pd.read_csv(deba.data("clean/cprr_benton_pd_2009_2014.csv"))
    cprr04 = pd.read_csv(deba.data("clean/cprr_benton_pd_2004_2008.csv"))
    agency = cprr21.agency[0]
    post = load_for_agency(agency)
    cprr21 = match_cprr21_and_post(cprr21, post)
    cprr14 = match_cprr14_and_post(cprr14, post)
    cprr04 = match_cprr04_and_post(cprr04, post)
    cprr21.to_csv(deba.data("match/cprr_benton_pd_2015_2021.csv"), index=False)
    cprr14.to_csv(deba.data("match/cprr_benton_pd_2009_2014.csv"), index=False)
    cprr04.to_csv(deba.data("match/cprr_benton_pd_2004_2008.csv"), index=False)
