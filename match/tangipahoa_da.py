from datamatch import JaroWinklerSimilarity, ThresholdMatcher, ColumnsIndex
import deba
from lib.post import load_for_agency
import pandas as pd


def match_brady_with_post(brady, post):
    dfa = brady[["first_name", "last_name", "uid"]]
    dfa.loc[:, "fc"] = dfa.first_name.map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = post[["first_name", "last_name", "uid"]]
    dfb.loc[:, "fc"] = dfb.first_name.map(lambda x: x[:1])
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
    decision = 1
    matcher.save_pairs_to_excel(
        deba.data("match/cprr_tangipahoa_da_2021_v_post_2020_11_06.xlsx"), decision
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    brady.loc[:, "uid"] = brady.uid.map(lambda x: match_dict.get(x, x))
    return brady


if __name__ == "__main__":
    cprr = pd.read_csv(deba.data("clean/cprr_tangipahoa_da_2021.csv"))
    agency = cprr.agency[0]
    post = load_for_agency(agency)
    cprr = match_cprr_with_post(cprr, post)
    cprr.to_csv(deba.data("match/cprr_tangipahoa_da_2021.csv"), index=False)
