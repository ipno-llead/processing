from lib.path import data_file_path, ensure_data_dir
from lib.uid import gen_uid
from lib.match import (
    ThresholdMatcher, ColumnsIndex, StringSimilarity, DateSimilarity
)
import pandas as pd
import sys
sys.path.append("../")


def match_uid_with_cprr(cprr, pprr):
    """
    match cprr and pprr records to add "uid" column to cprr
    """
    # construct "hire_date" column on cprr from appoint date columns
    # because appoint dates are similar to hire dates
    cprr_appoint_dates = cprr[["appoint_year", "appoint_month", "appoint_day"]]
    cprr_appoint_dates.columns = ["year", "month", "day"]
    cprr.loc[:, "hire_date"] = pd.to_datetime(cprr_appoint_dates)

    # generate column "mid" to merge
    cprr = gen_uid(cprr, ["first_name", "last_name",
                          "appoint_year", "appoint_month", "appoint_day"], "mid")

    # construct "hire_date" column
    pprr_hire_dates = pprr[["hire_year", "hire_month", "hire_day"]]
    pprr_hire_dates.columns = ["year", "month", "day"]
    pprr.loc[:, "hire_date"] = pd.to_datetime(pprr_hire_dates)

    # limit number of columns before matching
    dfa = cprr[["mid", "first_name", "last_name",
                "hire_date"]].drop_duplicates()
    dfa = dfa.set_index("mid", drop=True)
    dfb = pprr[["uid", "first_name", "last_name",
                "hire_date"]].drop_duplicates()
    dfb = dfb.set_index("uid", drop=True)
    matcher = ThresholdMatcher(dfa, dfb, ColumnsIndex(["first_name"]), {
        "last_name": StringSimilarity(),
        "hire_date": DateSimilarity()
    })
    decision = 0.5
    matcher.save_pairs_to_excel(data_file_path(
        "match/new_orleans_harbor_pd_cprr_2020_v_pprr_2020.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(decision)

    mid_to_uid_d = dict(matches)
    cprr.loc[:, "uid"] = cprr["mid"].map(lambda v: mid_to_uid_d[v])
    cprr = cprr.drop(columns=["mid", "hire_date"])
    return cprr


if __name__ == "__main__":
    cprr = pd.read_csv(data_file_path(
        "clean/cprr_new_orleans_harbor_pd_2020.csv"))
    pprr = pd.read_csv(data_file_path(
        "clean/pprr_new_orleans_harbor_pd_2020.csv"))
    cprr = match_uid_with_cprr(cprr, pprr)
    ensure_data_dir("match")
    cprr.to_csv(
        data_file_path("match/cprr_new_orleans_harbor_pd_2020.csv"),
        index=False)
