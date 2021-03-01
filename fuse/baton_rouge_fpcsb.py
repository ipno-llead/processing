from lib.path import data_file_path, ensure_data_dir
from lib.columns import rearrange_appeal_hearing_columns, rearrange_personnel_columns
import pandas as pd
import sys
sys.path.append("../")


def fuse():
    df = pd.read_csv(data_file_path(
        "clean/lprr_baton_rouge_fpcsb_1992_2012.csv"))
    return [
        rearrange_appeal_hearing_columns(df),
        rearrange_personnel_columns(df)
    ]


if __name__ == "__main__":
    [app_df, per_df] = fuse()

    ensure_data_dir("fuse")
    app_df.to_csv(data_file_path(
        "fuse/app_baton_rouge_fpcsb.csv"), index=False)
    per_df.to_csv(data_file_path(
        "fuse/per_baton_rouge_fpcsb.csv"), index=False)
