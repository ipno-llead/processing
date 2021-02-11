import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_complaint_columns, rearrange_personnel_columns, rearrange_personnel_history_columns
)
import sys
sys.path.append("../")


def fuse():
    cprr = pd.read_csv(data_file_path(
        "match/cprr_madisonville_pd_2010_2020.csv"))
    pprr = pd.read_csv(data_file_path("clean/pprr_madisonville_csd_2019.csv"))
    return (
        rearrange_personnel_columns(pprr),
        rearrange_personnel_history_columns(pprr),
        rearrange_complaint_columns(cprr)
    )


if __name__ == "__main__":
    per, perhist, com = fuse()
    ensure_data_dir("fuse")
    per.to_csv(data_file_path("fuse/per_madisonville_pd.csv"), index=False)
    perhist.to_csv(data_file_path(
        "fuse/perhist_madisonville_pd.csv"), index=False)
    com.to_csv(data_file_path("fuse/com_madisonville_pd.csv"), index=False)
