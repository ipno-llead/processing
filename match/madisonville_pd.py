from lib.match import (
    ThresholdMatcher, NoopIndex, JaroWinklerSimilarity
)
from lib.path import data_file_path, ensure_data_dir
import pandas as pd
import sys
sys.path.append("../")


def match(cprr, pprr):
    dfa = cprr[["last_name", "first_name"]]
    dfb = pprr[["last_name", "first_name", "uid"]].drop_duplicates()
    dfb = dfb.set_index("uid", drop=True)
    matcher = ThresholdMatcher(dfa, dfb, NoopIndex(), {
        "first_name": JaroWinklerSimilarity(),
        "last_name": JaroWinklerSimilarity()
    })
    decision = 1
    matcher.save_pairs_to_excel(data_file_path(
        "match/madisonville_pd_cprr_2010_2020_v_csd_pprr_2019.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    for idx, uid in matches:
        cprr.loc[idx, "uid"] = uid
    cprr = cprr.drop(columns=["first_name", "last_name"])
    return cprr


if __name__ == "__main__":
    cprr = pd.read_csv(data_file_path(
        "clean/cprr_madisonville_pd_2010_2020.csv"))
    pprr = pd.read_csv(data_file_path(
        "clean/pprr_madisonville_csd_2019.csv"))
    cprr = match(cprr, pprr)
    ensure_data_dir("match")
    cprr.to_csv(data_file_path(
        "match/cprr_madisonville_pd_2010_2020.csv"), index=False)
