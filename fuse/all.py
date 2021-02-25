import pandas as pd
from lib.path import data_file_path
from lib.columns import (
    rearrange_personnel_columns, rearrange_personnel_history_columns, rearrange_complaint_columns
)
import sys
sys.path.append("../")


def fuse_personnel():
    return rearrange_personnel_columns(pd.concat([
        pd.read_csv(data_file_path("fuse/per_new_orleans_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_new_orleans_harbor_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_baton_rouge_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_baton_rouge_so.csv")),
        pd.read_csv(data_file_path("fuse/per_new_orleans_csd.csv")),
        pd.read_csv(data_file_path("fuse/per_baton_rouge_csd.csv")),
        pd.read_csv(data_file_path("fuse/per_brusly_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_port_allen_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_madisonville_pd.csv")),
    ]))


def fuse_personnel_history():
    return rearrange_personnel_history_columns(pd.concat([
        pd.read_csv(data_file_path("fuse/perhist_new_orleans_pd.csv")),
        pd.read_csv(data_file_path("fuse/perhist_new_orleans_harbor_pd.csv")),
        pd.read_csv(data_file_path("fuse/perhist_baton_rouge_pd.csv")),
        pd.read_csv(data_file_path("fuse/perhist_baton_rouge_so.csv")),
        pd.read_csv(data_file_path("fuse/perhist_new_orleans_csd.csv")),
        pd.read_csv(data_file_path("fuse/perhist_baton_rouge_csd.csv")),
        pd.read_csv(data_file_path("fuse/perhist_brusly_pd.csv")),
        pd.read_csv(data_file_path("fuse/perhist_port_allen_pd.csv")),
        pd.read_csv(data_file_path("fuse/perhist_madisonville_pd.csv")),
    ]))


def fuse_complaint():
    return rearrange_complaint_columns(pd.concat([
        pd.read_csv(data_file_path("fuse/com_baton_rouge_pd.csv")),
        pd.read_csv(data_file_path("fuse/com_baton_rouge_so.csv")),
        pd.read_csv(data_file_path("fuse/com_brusly_pd.csv")),
        pd.read_csv(data_file_path("fuse/com_port_allen_pd.csv")),
        pd.read_csv(data_file_path("fuse/com_madisonville_pd.csv")),
    ]))


if __name__ == "__main__":
    per_df = fuse_personnel()
    perhist_df = fuse_personnel_history()
    com_df = fuse_complaint()
    per_df.to_csv(data_file_path("fuse/personnel.csv"), index=False)
    perhist_df.to_csv(data_file_path(
        "fuse/personnel_history.csv"), index=False)
    com_df.to_csv(data_file_path("fuse/complaint.csv"), index=False)
