from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex
from lib.path import data_file_path
from lib.post import load_for_agency
import pandas as pd
import sys

sys.path.append("../")


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
        data_file_path("match/cprr_greenwood_2015_2020_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


if __name__ == "__main__":
    cprr = pd.read_csv(data_file_path("clean/cprr_greenwood_pd_2015_2020.csv"))
    agency = cprr.agency[0]
    post = load_for_agency("clean/pprr_post_2020_11_06.csv", agency)
    cprr = match_cprr_and_post(cprr, post)
    cprr.to_csv(data_file_path("match/cprr_greenwood_pd_2015_2020.csv"), index=False)
