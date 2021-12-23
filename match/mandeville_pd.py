import sys

from datamatch import (
    ThresholdMatcher,
    ColumnsIndex,
    JaroWinklerSimilarity,
    DateSimilarity,
)
import pandas as pd

from lib.date import combine_date_columns
from lib.path import data_file_path, ensure_data_dir
from lib.post import extract_events_from_post

sys.path.append("../")


def match_cprr_with_pprr(cprr, pprr):
    dfa = (
        cprr[["uid", "first_name", "last_name"]]
        .drop_duplicates()
        .set_index("uid", drop=True)
    )
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = (
        pprr[["uid", "first_name", "last_name"]]
        .drop_duplicates()
        .set_index("uid", drop=True)
    )
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.89
    matcher.save_pairs_to_excel(
        data_file_path("match/mandeville_pd_cprr_2019_v_csd_pprr_2020.xlsx"), decision
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict[x])
    return cprr


def match_pprr_against_post(pprr, post):
    dfa = pprr[["uid", "first_name", "last_name"]]
    dfa.loc[:, "hire_date"] = combine_date_columns(
        pprr, "hire_year", "hire_month", "hire_day"
    )
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates().set_index("uid", drop=True)

    dfb = post[["uid", "first_name", "last_name"]]
    dfb.loc[:, "hire_date"] = combine_date_columns(
        post, "hire_year", "hire_month", "hire_day"
    )
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates().set_index("uid", drop=True)

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
            "hire_date": DateSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.8
    matcher.save_pairs_to_excel(
        data_file_path("match/mandeville_csd_pprr_2020_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    return extract_events_from_post(post, matches, "Mandeville PD")


if __name__ == "__main__":
    post = pd.read_csv(data_file_path("clean/pprr_post_2020_11_06.csv"))
    cprr = pd.read_csv(data_file_path("clean/cprr_mandeville_pd_2019.csv"))
    pprr = pd.read_csv(data_file_path("clean/pprr_mandeville_csd_2020.csv"))
    post_event = match_pprr_against_post(pprr, post)
    cprr = match_cprr_with_pprr(cprr, pprr)
    ensure_data_dir("match")
    post_event.to_csv(data_file_path("match/post_event_mandeville_pd_2019.csv"), index=False)
    cprr.to_csv(data_file_path("match/cprr_mandeville_pd_2019.csv"), index=False)
