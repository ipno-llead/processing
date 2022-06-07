import deba
from datamatch import ColumnsIndex, JaroWinklerSimilarity, ThresholdMatcher
import pandas as pd


def match_brady_to_personnel(brady, per):
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
    decision = 0.96
    matcher.save_pairs_to_excel(
        deba.data("match/brady_baton_da_2021_v_pprr_2021.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    brady.loc[:, "uid"] = brady.uid.map(lambda x: match_dict.get(x, x))
    return brady


if __name__ == "__main__":
    pprr = pd.read_csv(deba.data("match/pprr_baton_rouge_pd_2021.csv"))
    brady = pd.read_csv(deba.data("clean/brady_baton_rouge_da_2021.csv"))
    brady = match_brady_to_personnel(brady, pprr)
    brady.to_csv(deba.data("match/brady_baton_rouge_da_2021.csv"), index=False)
