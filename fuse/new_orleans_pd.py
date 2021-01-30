import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_personel_columns, rearrange_personel_history_columns
)

import sys
sys.path.append("../")


def fuse():
    df = pd.read_csv(
        data_file_path("clean/pprr_new_orleans_pd.csv"))
    return (
        rearrange_personel_columns(df),
        rearrange_personel_history_columns(df)
    )


if __name__ == "__main__":
    personel_df, history_df = fuse()
    ensure_data_dir("fuse")
    personel_df.to_csv(data_file_path(
        "fuse/per_new_orleans_pd.csv"), index=False)
    history_df.to_csv(data_file_path(
        "fuse/perhist_new_orleans_pd.csv"), index=False)
