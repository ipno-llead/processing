import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_personnel_columns, rearrange_personnel_history_columns, rearrange_complaint_columns
)

import sys
sys.path.append("../")


def fuse():
    df = pd.read_csv(
        data_file_path("match/cprr_baton_rouge_so_2018.csv"))
    return (
        rearrange_personnel_columns(df),
        rearrange_personnel_history_columns(df),
        rearrange_complaint_columns(df)
    )


if __name__ == "__main__":
    personnel_df, history_df, complaint_df = fuse()
    ensure_data_dir("fuse")
    personnel_df.to_csv(data_file_path(
        "fuse/per_baton_rouge_so.csv"), index=False)
    history_df.to_csv(data_file_path(
        "fuse/perhist_baton_rouge_so.csv"), index=False)
    complaint_df.to_csv(data_file_path(
        "fuse/com_baton_rouge_so.csv"), index=False)
