import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_personnel_columns, rearrange_personnel_history_columns, rearrange_complaint_columns
)

import sys
sys.path.append("../")


def fuse_personnel(csd_pprr_17, csd_pprr_19, pd_cprr_18):
    records = rearrange_personnel_columns(
        csd_pprr_17.set_index("uid", drop=False)).to_dict('index')
    for df in [csd_pprr_19, pd_cprr_18]:
        for idx, row in rearrange_personnel_columns(df.set_index("uid", drop=False)).iterrows():
            if idx in records:
                records[idx] = {
                    k: v if not pd.isnull(v) else row[k]
                    for k, v in records[idx].items() if k in row}
            else:
                records[idx] = row.to_dict()
    return rearrange_personnel_columns(pd.DataFrame.from_records(list(records.values())))


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
    personnel_df = fuse_personnel(csd_pprr_17, csd_pprr_19, pd_cprr_18)
    history_df = fuse_personnel_history(csd_pprr_17, csd_pprr_19)
    complaint_df = rearrange_complaint_columns(pd_cprr_18)

    ensure_data_dir("fuse")
    personnel_df.to_csv(data_file_path(
        "fuse/per_baton_rouge_pd.csv"), index=False)
    history_df.to_csv(data_file_path(
        "fuse/perhist_baton_rouge_pd.csv"), index=False)
    complaint_df.to_csv(data_file_path(
        "fuse/com_baton_rouge_pd.csv"), index=False)
