import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_personnel_columns, rearrange_personnel_history_columns, rearrange_complaint_columns
)

import sys
sys.path.append("../")


def fuse(pprr, cprr19):
    return (
        rearrange_personnel_columns(pprr),
        rearrange_personnel_history_columns(pd.concat([pprr, cprr19])),
        rearrange_complaint_columns(cprr19)
    )


if __name__ == "__main__":
    cprr19 = pd.read_csv(
        data_file_path("match/cprr_port_allen_pd_2019.csv"))
    pprr = pd.read_csv(data_file_path('match/pprr_port_allen_csd_2020.csv'))
    personnel_df, history_df, complaint_df = fuse(pprr, cprr19)
    ensure_data_dir("fuse")
    personnel_df.to_csv(data_file_path(
        "fuse/per_port_allen_pd.csv"), index=False)
    history_df.to_csv(data_file_path(
        "fuse/perhist_port_allen_pd.csv"), index=False)
    complaint_df.to_csv(data_file_path(
        "fuse/com_port_allen_pd.csv"), index=False)
