from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_complaint_columns, rearrange_personnel_columns,
    rearrange_personnel_history_columns
)
from lib.uid import gen_uid
import pandas as pd
import sys
sys.path.append('../')


if __name__ == '__main__':
    cprr = pd.read_csv(data_file_path('match/cprr_plaquemines_so_2019.csv'))
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    post = post[post.agency == 'plaquemines par so']
    post.loc[:, 'agency'] = 'Plaquemines SO'
    post = gen_uid(post, [
        'agency', 'uid', 'hire_year', 'hire_month', 'hire_day'
    ], 'perhist_uid')
    ensure_data_dir('fuse')
    rearrange_complaint_columns(cprr).to_csv(
        data_file_path('fuse/com_plaquemines_so.csv'),
        index=False
    )
    rearrange_personnel_columns(post).to_csv(
        data_file_path('fuse/per_plaquemines_so.csv'),
        index=False
    )
    rearrange_personnel_history_columns(post[post.hire_year.notna()]).to_csv(
        data_file_path('fuse/perhist_plaquemines_so.csv'),
        index=False
    )
