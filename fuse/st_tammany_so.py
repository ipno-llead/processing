from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_complaint_columns, rearrange_personnel_history_columns
)
from lib.personnel import fuse_personnel
import pandas as pd
import sys
sys.path.append('../')


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
