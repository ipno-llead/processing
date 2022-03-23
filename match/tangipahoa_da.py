from datamatch import JaroWinklerSimilarity, ThresholdMatcher, ColumnsIndex
import deba
import pandas as pd


def match_brady_to_personnel(brady, per):
    dfa = brady[["first_name", "last_name", "uid"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = per[["first_name", "last_name", "uid"]]
    dfb.loc[:, "first_name"] = dfb.first_name.str.lower().str.strip()
    dfb.loc[:, "last_name"] = dfb.last_name.str.lower().str.strip()
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc", "lc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.967
    matcher.save_pairs_to_excel(
        deba.data("match/cprr_tangipahoa_da_2021_v_personnel.xlsx"), decision
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    brady.loc[:, "uid"] = brady.uid.map(lambda x: match_dict.get(x, x))
    return brady


if __name__ == "__main__":
    per = pd.read_csv(deba.data("raw/fused/personnel.csv"))
    brady = pd.read_csv(deba.data("clean/brady_tangipahoa_da_2021.csv"))
    brady = match_brady_to_personnel(brady, per)
    brady.to_csv(deba.data("match/brady_tangipahoa_da_2021.csv"), index=False)
