import bolo
from lib.post import load_for_agency
from datamatch import ColumnsIndex, JaroWinklerSimilarity, ThresholdMatcher
import pandas as pd


def match_cprr_18_against_post(cprr, post):
    dfa = cprr[["uid", "first_name", "last_name"]]
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
    decision = 1
    matcher.save_pairs_to_excel(
        bolo.data("match/baton_rouge_so_cprr_2018_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)
    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_cprr_20_against_post(cprr, post):
    dfa = cprr[["uid", "first_name", "last_name"]]
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
    decision = 1
    matcher.save_pairs_to_excel(
        bolo.data("match/baton_rouge_so_cprr_2020_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)
    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


if __name__ == "__main__":
    cprr18 = pd.read_csv(bolo.data("clean/cprr_baton_rouge_so_2018.csv"))
    cprr20 = pd.read_csv(bolo.data("clean/cprr_baton_rouge_so_2016_2020.csv"))
    agency = cprr20.agency[0]
    post = load_for_agency(agency)
    cprr18 = match_cprr_18_against_post(cprr18, post)
    cprr20 = match_cprr_20_against_post(cprr20, post)

    cprr18.to_csv(bolo.data("match/cprr_baton_rouge_so_2018.csv"), index=False)
    cprr20.to_csv(bolo.data("match/cprr_baton_rouge_so_2016_2020.csv"), index=False)
