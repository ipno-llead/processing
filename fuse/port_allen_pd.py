import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_personnel_columns, rearrange_personnel_history_columns, rearrange_complaint_columns
)

import sys
sys.path.append("../")


if __name__ == "__main__":
    cprr19 = pd.read_csv(
        data_file_path("match/cprr_port_allen_pd_2019.csv"))
    cprr18 = pd.read_csv(
        data_file_path("match/cprr_port_allen_pd_2017_2018.csv"))
    cprr16 = pd.read_csv(
        data_file_path("match/cprr_port_allen_pd_2015_2016.csv"))
    pprr = pd.read_csv(data_file_path('match/pprr_port_allen_csd_2020.csv'))
    personnel_df = rearrange_personnel_columns(pprr)
    history_df = rearrange_personnel_history_columns(pprr)
    complaint_df = rearrange_complaint_columns(
        pd.concat([cprr16, cprr18, cprr19]))
    ensure_data_dir("fuse")
    personnel_df.to_csv(data_file_path(
        "fuse/per_port_allen_pd.csv"), index=False)
    history_df.to_csv(data_file_path(
        "fuse/perhist_port_allen_pd.csv"), index=False)
    complaint_df.to_csv(data_file_path(
        "fuse/com_port_allen_pd.csv"), index=False)
