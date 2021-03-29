import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_personnel_columns, rearrange_personnel_history_columns, rearrange_complaint_columns
)
from lib.uid import gen_uid

import sys
sys.path.append("../")


def fuse():
    df = pd.read_csv(
        data_file_path("clean/pprr_brusly_pd_2020.csv"))
    return (
        rearrange_personnel_columns(df),
        rearrange_personnel_history_columns(df),
    )


if __name__ == "__main__":
    personnel_df, history_df = fuse()
    com_df = pd.read_csv(data_file_path("match/cprr_brusly_pd_2020.csv"))
    com_df = gen_uid(com_df, [
        'uid', 'occur_year', 'occur_month', 'occur_day'], 'complaint_uid')
    com_df = rearrange_complaint_columns(com_df)
    ensure_data_dir("fuse")
    personnel_df.to_csv(data_file_path(
        "fuse/per_brusly_pd.csv"), index=False)
    history_df.to_csv(data_file_path(
        "fuse/perhist_brusly_pd.csv"), index=False)
    com_df.to_csv(data_file_path(
        "fuse/com_brusly_pd.csv"
    ), index=False)
