import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.personnel import fuse_personnel
from lib.columns import (
    rearrange_personnel_history_columns, rearrange_complaint_columns,
    rearrange_appeal_hearing_columns
)

import sys
sys.path.append("../")


def fuse_personnel_history(csd_pprr_17, csd_pprr_19):
    return pd.concat([
        rearrange_personnel_history_columns(csd_pprr_17),
        rearrange_personnel_history_columns(csd_pprr_19)])


if __name__ == "__main__":
    csd_pprr_17 = pd.read_csv(
        data_file_path("match/pprr_baton_rouge_csd_2017.csv")
    )
    csd_pprr_19 = pd.read_csv(
        data_file_path("match/pprr_baton_rouge_csd_2019.csv")
    )
    pd_cprr_18 = pd.read_csv(
        data_file_path("match/cprr_baton_rouge_pd_2018.csv"))
    lprr = pd.read_csv(data_file_path(
        "match/lprr_baton_rouge_fpcsb_1992_2012.csv"))
    personnel_df = fuse_personnel(csd_pprr_17, csd_pprr_19, pd_cprr_18, lprr)
    history_df = fuse_personnel_history(csd_pprr_17, csd_pprr_19)
    complaint_df = rearrange_complaint_columns(pd_cprr_18)
    lprr_df = rearrange_appeal_hearing_columns(lprr)

    ensure_data_dir("fuse")
    personnel_df.to_csv(data_file_path(
        "fuse/per_baton_rouge_pd.csv"), index=False)
    history_df.to_csv(data_file_path(
        "fuse/perhist_baton_rouge_pd.csv"), index=False)
    complaint_df.to_csv(data_file_path(
        "fuse/com_baton_rouge_pd.csv"), index=False)
    lprr_df.to_csv(data_file_path("fuse/app_baton_rouge_pd.csv"), index=False)
