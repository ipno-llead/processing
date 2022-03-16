import pandas as pd
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex

import deba


def match_pd_pprr_with_csd_pprr(pdpprr, csdpprr):
    dfa = (
        pdpprr[["uid", "first_name", "last_name"]]
        .drop_duplicates()
        .set_index("uid", drop=True)
    )
    dfa.loc[:, "fc"] = dfa.first_name.map(lambda x: x[:1])

    dfb = (
        csdpprr[["uid", "first_name", "last_name"]]
        .drop_duplicates()
        .set_index("uid", drop=True)
    )
    dfb.loc[:, "fc"] = dfb.first_name.map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.97
    matcher.save_pairs_to_excel(
        deba.data("match/harahan_pd_pprr_2020_v_csd_pprr_2020.xlsx"), decision
    )


if __name__ == "__main__":
    pdpprr = pd.read_csv(deba.data("clean/pprr_harahan_pd_2020.csv"))
    csdpprr = pd.read_csv(deba.data("clean/pprr_harahan_csd_2020.csv"))
    match_pd_pprr_with_csd_pprr(pdpprr, csdpprr)
