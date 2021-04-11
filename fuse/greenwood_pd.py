from lib.columns import (
    rearrange_complaint_columns, rearrange_personnel_columns, rearrange_personnel_history_columns
)
from lib.path import data_file_path, ensure_data_dir
from lib.uid import gen_uid
import pandas as pd
import sys
sys.path.append('../')


def fuse(cprr, post_pprr):
    post_pprr = post_pprr.loc[
        (post_pprr.agency == 'greenwood pd')
    ]
    post_pprr.loc[:, 'data_production_year'] = '2020'
    post_pprr.loc[:, 'agency'] = 'Greenwood PD'
    cprr = gen_uid(cprr, [
        'uid', 'complaint_uid'
    ], 'allegation_uid')
    cprr.loc[:, 'data_production_year'] = '2020'
    cprr.loc[:, 'agency'] = 'Greenwood PD'
    return (
        rearrange_personnel_columns(post_pprr),
        rearrange_complaint_columns(cprr),
        rearrange_personnel_history_columns(
            post_pprr[post_pprr.hire_year.notna()])
    )


if __name__ == '__main__':
    cprr = pd.read_csv(
        data_file_path('match/cprr_greenwood_pd_2015_2020.csv')
    )
    post_pprr = pd.read_csv(
        data_file_path('clean/pprr_post_2020_11_06.csv')
    )
    per, com, perhist = fuse(cprr, post_pprr)
    ensure_data_dir('fuse')
    per.to_csv(
        data_file_path('fuse/per_greenwood_pd.csv'),
        index=False
    )
    com.to_csv(
        data_file_path('fuse/com_greenwood_pd.csv'),
        index=False
    )
    perhist.to_csv(
        data_file_path('fuse/perhist_greenwood_pd.csv'),
        index=False
    )
