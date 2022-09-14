import pandas as pd
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex

import deba


def match_pprr_and_csd(pprr, csd):
    dfa = pprr[["uid", "first_name", "last_name"]]
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = csd[["uid", "first_name", "last_name"]]
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
        deba.data("match/pprr_harahan_pd_v_harahan_csd_2020.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    pprr.loc[:, "uid"] = pprr.uid.map(lambda x: match_dict.get(x, x))
    return pprr


if __name__ == "__main__":
    pprr_pd = pd.read_csv(deba.data("clean/pprr_harahan_pd_2020.csv"))
    pprr_csd = pd.read_csv(deba.data("clean/pprr_harahan_csd_2020.csv"))
    pprr = match_pprr_and_csd(pprr_pd, pprr_csd)
    pprr.to_csv(deba.data("match/pprr_harahan_pd_2020.csv"), index=False)
