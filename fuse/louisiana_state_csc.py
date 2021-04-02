from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_appeal_hearing_columns, rearrange_personnel_columns, rearrange_personnel_history_columns
)
import pandas as pd
import sys
sys.path.append('../')


def fuse_personnel(lprr, post):
    records = rearrange_personnel_columns(
        post.set_index("uid", drop=False)).to_dict('index')
    for idx, row in rearrange_personnel_columns(lprr.set_index("uid", drop=False)).iterrows():
        if idx in records:
            records[idx] = {
                k: v if not pd.isnull(v) else row[k]
                for k, v in records[idx].items() if k in row}
        else:
            records[idx] = row.to_dict()
    return rearrange_personnel_columns(pd.DataFrame.from_records(list(records.values())))


if __name__ == '__main__':
    lprr = pd.read_csv('match/lprr_louisiana_state_csc_1991_2020.csv')
    post = pd.read_csv('clean/pprr_post_2020_11_06.csv')
    post = post[post.agency == 'la state police']
    post.loc[:, 'agency'] = 'Louisiana State Police'
    per_df = fuse_personnel(lprr, post)
    ensure_data_dir('fuse')
    per_df.to_csv(data_file_path(
        'fuse/per_louisiana_csc.csv'
    ), index=False)
    rearrange_personnel_history_columns(post).to_csv(data_file_path(
        'fuse/perhist_louisiana_csc.csv'
    ), index=False)
    rearrange_appeal_hearing_columns(data_file_path(
        'fuse/app_louisiana_csc.csv'
    ), index=False)
