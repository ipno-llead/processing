import deba
from datamatch import ColumnsIndex, JaroWinklerSimilarity, ThresholdMatcher
import pandas as pd


def match_brady21_to_pprr(brady, per):
    dfa = brady[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = per[["uid", "first_name", "last_name"]]
    dfb.loc[:, "first_name"] = dfb.first_name.str.lower().str.strip()
    dfb.loc[:, "last_name"] = dfb.last_name.str.lower().str.strip()
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc", "lc"]),
        {
            "last_name": JaroWinklerSimilarity(),
            "first_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.948
    matcher.save_pairs_to_excel(
        deba.data("match/brady_baton_da_2021_v_pprr_baton_rouge_pd_2021.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    brady.loc[:, "uid"] = brady.uid.map(lambda x: match_dict.get(x, x))
    return brady


def match_brady18_to_post(brady, post):
    dfa = brady[["uid", "first_name", "last_name", "agency"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = post[["uid", "first_name", "last_name", "agency"]]
    dfb.loc[:, "first_name"] = dfb.first_name.str.lower().str.strip()
    dfb.loc[:, "last_name"] = dfb.last_name.str.lower().str.strip()
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc", "lc", "agency"]),
        {
            "last_name": JaroWinklerSimilarity(),
            "first_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.958
    matcher.save_pairs_to_excel(
        deba.data("match/brady_baton_da_2018_v_pprr_baton_rouge_pd_2021.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    brady.loc[:, "uid"] = brady.uid.map(lambda x: match_dict.get(x, x))
    return brady


if __name__ == "__main__":
    pprr = pd.read_csv(deba.data("match/pprr_baton_rouge_pd_2021.csv"))
    post = pd.read_csv(deba.data("clean/pprr_post_2020_11_06.csv"))
    brady21 = pd.read_csv(deba.data("clean/brady_baton_rouge_da_2021.csv"))
    brady18  = pd.read_csv(deba.data("clean/brady_baton_rouge_da_2018.csv"))
    brady21 = match_brady21_to_pprr(brady21, pprr)
    brady18 = match_brady18_to_post(brady18, pprr)
    brady21.to_csv(deba.data("match/brady_baton_rouge_da_2021.csv"), index=False)
    brady18.to_csv(deba.data("match/brady_baton_rouge_da_2018.csv"), index=False)
