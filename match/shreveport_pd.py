import sys

import pandas as pd
from datamatch import (
    ThresholdMatcher,
    JaroWinklerSimilarity,
    ColumnsIndex,
    StringSimilarity,
)

from lib.path import data_file_path, ensure_data_dir
from lib.uid import gen_uid

sys.path.append("../")


def match_cprr_codebook(cprr, cb):
    dfa = cprr[["allegation"]]
    allegation_df = cprr.allegation.str.extract(r"(\d{3}\.\d{1,2})(.+)$")
    dfa.loc[:, "name"] = allegation_df.loc[:, 1].str.strip()
    dfa.loc[:, "code"] = allegation_df.loc[:, 0]
    dfa = dfa[dfa.code.notna()].drop_duplicates().set_index("allegation", drop=True)

    matcher = ThresholdMatcher(
        ColumnsIndex("code"), {"name": StringSimilarity()}, dfa, cb
    )
    decision = 0.541
    lower_bound = 0.5
    matcher.save_pairs_to_excel(
        data_file_path("match/shreveport_pd_cprr_2018_2019_v_codebook.xlsx"),
        decision,
        lower_bound=lower_bound,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    code_n_name = cb.code.str.cat(cb.name, sep=" ")
    cprr.loc[:, "allegation"] = cprr.allegation.map(
        lambda x: x if x not in match_dict else code_n_name[match_dict[x]]
    )
    return cprr


def match_cprr_post(cprr, post):
    dfa = cprr[["uid", "first_name", "last_name"]]
    dfa = dfa.drop_duplicates().set_index("uid")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = post[["uid", "first_name", "last_name"]]
    dfb = dfb.drop_duplicates().set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex("fc"),
        {"first_name": JaroWinklerSimilarity(), "last_name": JaroWinklerSimilarity()},
        dfa,
        dfb,
    )
    decision = 0.95
    matcher.save_pairs_to_excel(
        data_file_path(
            "match/shreveport_pd_cprr_2018_2019_v_post_pprr_2020_11_06.xlsx"
        ),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


if __name__ == "__main__":
    cprr = pd.read_csv(data_file_path("clean/cprr_shreveport_pd_2018_2019.csv"))
    post = pd.read_csv(data_file_path("clean/pprr_post_2020_11_06.csv"))
    cb = pd.read_csv(data_file_path("clean/cprr_codebook_shreveport_pd.csv"))
    cprr = match_cprr_codebook(cprr, cb).pipe(
        gen_uid, ["agency", "tracking_number", "allegation"], "allegation_uid"
    )
    cprr = match_cprr_post(cprr, post)
    ensure_data_dir("match")
    cprr.to_csv(data_file_path("match/cprr_shreveport_pd_2018_2019.csv"), index=False)
