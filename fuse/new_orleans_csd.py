import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_personnel_columns, rearrange_personnel_history_columns
)

import sys
sys.path.append("../")


def fuse():
    df09 = pd.read_csv(
        data_file_path("match/pprr_new_orleans_csd_2009.csv")
    )
    df14 = pd.read_csv(
        data_file_path("match/pprr_new_orleans_csd_2014.csv")
    )
    df = pd.concat([df09, df14])
    return (
        rearrange_personnel_columns(df),
        rearrange_personnel_history_columns(df)
    )


if __name__ == "__main__":
    personnel_df, history_df = fuse()
    ensure_data_dir("fuse")
    personnel_df.to_csv(data_file_path(
        "fuse/per_new_orleans_csd.csv"), index=False)
    history_df.to_csv(data_file_path(
        "fuse/perhist_new_orleans_csd.csv"), index=False)
