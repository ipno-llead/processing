from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex
from lib.path import data_file_path
import pandas as pd
import sys

sys.path.append("../")


def match_brady_and_post(brady, post):
    dfa = (
        brady.loc[brady.uid.notna(), ["uid", "first_name", "last_name"]]
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
    decision = 0.94
    matcher.save_pairs_to_excel(
        data_file_path("match/brady_new_orleans_da_2021_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    brady.loc[:, "uid"] = brady.uid.map(lambda x: match_dict.get(x, x))
    return brady


if __name__ == "__main__":
    pprr = pd.read_csv(data_file_path("clean/pprr_new_orleans_pd_1946_2018.csv"))
    brady = pd.read_csv(data_file_path("clean/brady_new_orleans_da_2021.csv"))
    brady = match_brady_and_post(brady, pprr)
    brady.to_csv(data_file_path("match/brady_new_orleans_da_2021.csv"), index=False)
