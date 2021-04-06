from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_appeal_hearing_columns, rearrange_personnel_history_columns
)
from lib.post import keep_latest_row_for_each_post_officer
from lib.personnel import fuse_personnel
import pandas as pd
import sys
sys.path.append('../')


if __name__ == '__main__':
    lprr = pd.read_csv(data_file_path(
        'match/lprr_louisiana_state_csc_1991_2020.csv'))
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    post = post[post.agency == 'la state police']
    post.loc[:, 'agency'] = 'Louisiana State Police'
    per_df = fuse_personnel(lprr, keep_latest_row_for_each_post_officer(post))
    ensure_data_dir('fuse')
    per_df.to_csv(data_file_path(
        'fuse/per_louisiana_state_police.csv'
    ), index=False)
    rearrange_personnel_history_columns(post).to_csv(data_file_path(
        'fuse/perhist_louisiana_state_police.csv'
    ), index=False)
    rearrange_appeal_hearing_columns(lprr).to_csv(data_file_path(
        'fuse/app_louisiana_state_police.csv'
    ), index=False)
