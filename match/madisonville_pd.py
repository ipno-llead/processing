from lib.match import (
    match_records, ThresholdClassifier, NoopIndex, JaroWinklerSimilarity,
    print_match_result
)
from lib.path import data_file_path, ensure_data_dir
import pandas as pd
import sys
sys.path.append("../")


def match(cprr, pprr, print_result=False):
    dfa = cprr[["last_name", "first_name"]]
    dfb = pprr[["last_name", "first_name", "uid"]].drop_duplicates()
    dfb = dfb.set_index("uid", drop=True)
    matches, potential_matches, non_matches = match_records(
        NoopIndex(),
        ThresholdClassifier(
            fields={
                "first_name": JaroWinklerSimilarity(),
                "last_name": JaroWinklerSimilarity()
            },
            thresholds=[1, 0.7]
        ),
        dfa,
        dfb
    )

    if print_result:
        print_match_result(dfa, dfb, matches, potential_matches, non_matches)
    else:
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
