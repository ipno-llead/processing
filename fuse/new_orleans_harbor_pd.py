import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_personnel_columns, rearrange_personnel_history_columns,
    rearrange_complaint_columns
)

import sys
sys.path.append("../")


def fuse():
    pprr20 = pd.read_csv(
        data_file_path("match/pprr_new_orleans_harbor_pd_2020.csv")
    )
    pprr08 = pd.read_csv(
        data_file_path("clean/pprr_new_orleans_harbor_pd_1991_2008.csv")
    )
    cprr = pd.read_csv(
        data_file_path("match/cprr_new_orleans_harbor_pd_2020.csv")
    )
    personnel = rearrange_personnel_columns(pd.concat([pprr08, pprr20]))
    personnel_history = rearrange_personnel_history_columns(pd.concat(
        [pprr08, pprr20]))
    complaint = rearrange_complaint_columns(cprr)
    return (
        personnel,
        personnel_history,
        complaint
    )


if __name__ == "__main__":
    personnel_df, history_df, complaint_df = fuse()
    ensure_data_dir("fuse")
    personnel_df.to_csv(data_file_path(
        "fuse/per_new_orleans_harbor_pd.csv"), index=False)
    history_df.to_csv(data_file_path(
        "fuse/perhist_new_orleans_harbor_pd.csv"), index=False)
    complaint_df.to_csv(data_file_path(
        "fuse/com_new_orleans_harbor_pd.csv"), index=False)
