import pandas as pd
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex
import deba
from lib.post import load_for_agency


def match_cprr_post(cprr, post):
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
    decision = 0.95
    matcher.save_pairs_to_excel(
        deba.data("match/cprr_jefferson_davis_so_v_pprr_post_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


if __name__ == "__main__":
    cprr = pd.read_csv(deba.data("clean/cprr_jefferson_davis_so_2013_2022.csv"))
    agency = cprr.agency[0]
    post = load_for_agency(agency)
    cprr = match_cprr_post(cprr, post)
    cprr.to_csv(deba.data("match/cprr_jefferson_davis_so_2013_2022.csv"), index=False)
