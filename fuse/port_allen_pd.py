import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_personnel_columns, rearrange_personnel_history_columns, rearrange_complaint_columns
)

import sys
sys.path.append("../")


def fuse():
    df = pd.read_csv(
        data_file_path("clean/cprr_port_allen_pd_2019.csv"))
    return (
        rearrange_personnel_columns(df),
        rearrange_personnel_history_columns(df),
        rearrange_complaint_columns(df)
    )


if __name__ == "__main__":
    personnel_df, history_df, complaint_df = fuse()
    ensure_data_dir("fuse")
    personnel_df.to_csv(data_file_path(
        "fuse/per_port_allen_pd.csv"), index=False)
    history_df.to_csv(data_file_path(
        "fuse/perhist_port_allen_pd.csv"), index=False)
    complaint_df.to_csv(data_file_path(
        "fuse/com_port_allen_pd.csv"), index=False)
