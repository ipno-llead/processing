from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_complaint_columns, rearrange_personnel_columns, rearrange_personnel_history_columns
)
import pandas as pd
import sys
sys.path.append('../')


def fuse_personnel(pprr, cprr):
    records = rearrange_personnel_columns(
        pprr.set_index("uid", drop=False)).to_dict('index')
    for idx, row in rearrange_personnel_columns(cprr.set_index("uid", drop=False)).iterrows():
        if idx in records:
            records[idx] = {
                k: v if not pd.isnull(v) else row[k]
                for k, v in records[idx].items() if k in row}
        else:
            records[idx] = row.to_dict()
    return rearrange_personnel_columns(pd.DataFrame.from_records(list(records.values())))


if __name__ == '__main__':
    cprr = pd.read_csv(data_file_path(
        'match/cprr_st_tammany_so_2011_2021.csv'
    ))
    pprr = pd.read_csv(data_file_path(
        'match/pprr_st_tammany_so_2020.csv'
    ))
    personnels = fuse_personnel(pprr, cprr)
    perhist = rearrange_personnel_history_columns(pd.concat([pprr, cprr]))
    complaints = rearrange_complaint_columns(cprr)
    ensure_data_dir('fuse')
    personnels.to_csv(data_file_path(
        'fuse/per_st_tammany_so.csv'), index=False)
    perhist.to_csv(data_file_path(
        'fuse/perhist_st_tammany_so.csv'), index=False)
    complaints.to_csv(data_file_path(
        'fuse/com_st_tammany_so.csv'), index=False)
