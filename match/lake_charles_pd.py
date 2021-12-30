import sys

sys.path.append("../")
import pandas as pd
from datamatch import JaroWinklerSimilarity, ThresholdMatcher, ColumnsIndex
from lib.path import data_file_path
from lib.post import load_for_agency


def assign_uid_from_post_20(cprr, post):
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
    decision = 0.93
    matcher.save_pairs_to_excel(
        data_file_path("match/lake_charles_pd_cprr_20_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def assign_uid_from_post_19(cprr, post):
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
    decision = 0.86
    matcher.save_pairs_to_excel(
        data_file_path(
            "match/lake_charles_pd_cprr_2014_2019_v_post_pprr_2020_11_06.xlsx"
        ),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


if __name__ == "__main__":
    cprr_20 = pd.read_csv(data_file_path("clean/cprr_lake_charles_pd_2020.csv"))
    cprr_19 = pd.read_csv(data_file_path("clean/cprr_lake_charles_pd_2014_2019.csv"))
    agency = cprr_19.agency[0]
    post = load_for_agency("clean/pprr_post_2020_11_06.csv", agency)
    cprr_20 = assign_uid_from_post_20(cprr_20, post)
    cprr_19 = assign_uid_from_post_19(cprr_19, post)
    cprr_20.to_csv(data_file_path("match/cprr_lake_charles_pd_2020.csv"), index=False)
    cprr_19.to_csv(
        data_file_path("match/cprr_lake_charles_pd_2014_2019.csv"), index=False
    )
