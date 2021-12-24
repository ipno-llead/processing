import sys

sys.path.append("../")
from lib.path import data_file_path
import pandas as pd
from datamatch import JaroWinklerSimilarity, ThresholdMatcher, ColumnsIndex


def match_cprr_21_with_post(cprr, post):
    dfa = cprr[["uid", "first_name", "last_name"]]
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = post[["uid", "first_name", "last_name"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index(["uid"])
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
    decision = 0.97
    matcher.save_pairs_to_excel(
        data_file_path("match/cprr_houma_pd_2019_2021_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_cprr_18_with_post(cprr, post):
    dfa = cprr[["uid", "first_name", "last_name"]]
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = post[["uid", "first_name", "last_name"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index(["uid"])
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
    decision = 0.87
    matcher.save_pairs_to_excel(
        data_file_path("match/cprr_houma_pd_2017_2019_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


if __name__ == "__main__":
    cprr21 = pd.read_csv(data_file_path("clean/cprr_houma_pd_2019_2021.csv"))
    cprr18 = pd.read_csv(data_file_path("clean/cprr_houma_pd_2017_2018.csv"))
    agency = cprr18.agency[0]
    post = pd.read_csv(data_file_path("clean/pprr_post_2020_11_06.csv"))
    post = post.loc[post.agency == agency]
    cprr21 = match_cprr_21_with_post(cprr21, post)
    cprr18 = match_cprr_18_with_post(cprr18, post)
    cprr21.to_csv(data_file_path("match/cprr_houma_pd_2019_2021.csv"), index=False)
    cprr18.to_csv(data_file_path("match/cprr_houma_pd_2017_2018.csv"), index=False)
