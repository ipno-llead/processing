import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_personnel_columns, rearrange_personnel_history_columns,
    rearrange_complaint_columns
)

import sys
sys.path.append("../")


def fuse():
    pprr = pd.read_csv(
        data_file_path("clean/pprr_new_orleans_harbor_pd_2020.csv")
    )
    cprr = pd.read_csv(
        data_file_path("match/cprr_new_orleans_harbor_pd_2020.csv")
    )
    personnel = rearrange_personnel_columns(pprr)
    personnel_history_1 = rearrange_personnel_history_columns(pprr)
    personnel_history_2 = rearrange_personnel_history_columns(cprr)
    complaint = rearrange_complaint_columns(cprr)
    return (
        personnel,
        rearrange_personnel_history_columns(
            pd.concat([personnel_history_1, personnel_history_2])),
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
