from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex
import deba
from lib.post import load_for_agency
import pandas as pd


def match_cprr_and_post(cprr, post):
    dfa = (
        cprr.loc[cprr.uid.notna(), ["uid", "first_name", "last_name"]]
        .drop_duplicates(subset=["uid"])
        .set_index("uid", drop=True)
    )
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = (
        post[["uid", "first_name", "last_name"]]
        .drop_duplicates()
        .set_index("uid", drop=True)
    )
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
    decision = 0.96
    matcher.save_pairs_to_excel(
        deba.data("match/cprr_greenwood_2015_2020_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr

def match_pprr_and_post(pprr, post):
    dfa = (
        pprr.loc[pprr.uid.notna(), ["uid", "first_name", "last_name"]]
        .drop_duplicates(subset=["uid"])
        .set_index("uid", drop=True)
    )
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = (
        post[["uid", "first_name", "last_name"]]
        .drop_duplicates()
        .set_index("uid", drop=True)
    )
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
    decision = 0.96
    matcher.save_pairs_to_excel(
        deba.data("match/pprr_greenwood_1990_2001_v_post.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    pprr.loc[:, "uid"] = pprr.uid.map(lambda x: match_dict.get(x, x))
    return pprr


if __name__ == "__main__":
    cprr = pd.read_csv(deba.data("clean/cprr_greenwood_pd_2015_2020.csv"))
    pprr = pd.read_csv(deba.data("clean/pprr_greenwood_pd_1990_2001.csv"))
    agency = cprr.agency[0]
    post = load_for_agency(agency)
    cprr = match_cprr_and_post(cprr, post)
    pprr = match_pprr_and_post(pprr, post)
    cprr.to_csv(deba.data("match/cprr_greenwood_pd_2015_2020.csv"), index=False)
    pprr.to_csv(deba.data("match/pprr_greenwood_pd_1990_2001.csv"), index=False)
