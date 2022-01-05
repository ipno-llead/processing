import sys

from lib.clean import canonicalize_names

sys.path.append("../")
import pandas as pd
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex
from lib.path import data_file_path
from lib.post import load_for_agency


def deduplicate_cprr_officers(cprr):
    df = cprr[["uid", "first_name", "last_name", "rank_desc"]]
    df = df.drop_duplicates(subset=["uid"]).set_index("uid")
    df.loc[:, "fc"] = df.first_name.fillna("").map(lambda x: x[:1])
    matcher = ThresholdMatcher(
        ColumnsIndex("fc"),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        df,
    )
    decision = 0.866
    matcher.save_clusters_to_excel(
        data_file_path("match/tangipahoa_so_cprr_2015_2021_deduplicate.xlsx"),
        decision,
        decision,
    )
    clusters = matcher.get_index_clusters_within_thresholds(decision)
    # canonicalize name and uid
    canonicalize_names(cprr, clusters)
    return cprr


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
    decision = 0.873
    matcher.save_pairs_to_excel(
        data_file_path(
            "match/tangipahoa_so_cprr_2015_2021_v_pprr_post_2020_11_06.xlsx"
        ),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


if __name__ == "__main__":
    cprr = pd.read_csv(data_file_path("clean/cprr_tangipahoa_so_2015_2021.csv"))
    agency = cprr.agency[0]
    post = load_for_agency("clean/pprr_post_2020_11_06.csv", agency)
    cprr = deduplicate_cprr_officers(cprr)
    cprr = match_cprr_post(cprr, post)
    cprr.to_csv(data_file_path("match/cprr_tangipahoa_so_2015_2021.csv"), index=False)
