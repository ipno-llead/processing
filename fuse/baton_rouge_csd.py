import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_personnel_columns, rearrange_personnel_history_columns
)

import sys
sys.path.append("../")


def fuse():
    df17 = pd.read_csv(
        data_file_path("match/pprr_baton_rouge_csd_2017.csv")
    )
    df19 = pd.read_csv(
        data_file_path("match/pprr_baton_rouge_csd_2019.csv")
    )
    df = pd.concat([df17, df19])
    return (
        rearrange_personnel_columns(df),
        rearrange_personnel_history_columns(df)
    )


if __name__ == "__main__":
    personnel_df, history_df = fuse()
    ensure_data_dir("fuse")
    personnel_df.to_csv(data_file_path(
        "fuse/per_baton_rouge_csd.csv"), index=False)
    history_df.to_csv(data_file_path(
        "fuse/perhist_baton_rouge_csd.csv"), index=False)
