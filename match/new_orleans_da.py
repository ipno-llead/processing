from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex
from lib.path import data_file_path, ensure_data_dir
import pandas as pd
import sys

sys.path.append("../")


def prepare_new_orleans_so_post_data():
    post = pd.read_csv(data_file_path("clean/pprr_post_2020_11_06.csv"))
    return post[post.agency == "orleans parish so"]


def prepare_new_orleans_pd_post_data():
    post = pd.read_csv(data_file_path("clean/pprr_post_2020_11_06.csv"))
    return post[post.agency == "new orleans pd"]


def match_cprr_and_nopd_post(cprr, post):
    dfa = cprr[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = post[["uid", "first_name", "last_name"]]
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "last_name": JaroWinklerSimilarity(),
            "first_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 1
    matcher.save_pairs_to_excel(
        data_file_path(
            "match/new_orleans_da_cprr_2021_v_post_nopd_pprr_2020_11_06.xlsx"
        ),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_against_new_orleans_pd_personnel(cprr, pprr):
    dfa = cprr[["first_name", "last_name", "uid"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = pprr[["first_name", "last_name", "uid"]]
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid", drop=True)

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "last_name": JaroWinklerSimilarity(),
            "first_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 1
    matcher.save_pairs_to_excel(
        data_file_path(
            "match/new_orleans_da_cprr_2021_v_new_orleans_pd_1946_2018.xlsx"
        ),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr["uid"].map(lambda v: match_dict.get(v, v))
    return cprr


if __name__ == "__main__":
    pprr = pd.read_csv(data_file_path("clean/pprr_new_orleans_pd_1946_2018.csv"))
    cprr = pd.read_csv(data_file_path("clean/cprr_new_orleans_da_2021.csv"))
    cprr = match_against_new_orleans_pd_personnel(cprr, pprr)
    post_noso = prepare_new_orleans_so_post_data()
    cprr = match_cprr_and_nopd_post(cprr, post_noso)
    ensure_data_dir("match")
    cprr.to_csv(data_file_path("match/cprr_new_orleans_da_2021.csv"), index=False)
