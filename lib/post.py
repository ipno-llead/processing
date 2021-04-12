from lib.path import data_file_path
import pandas as pd
import sys
sys.path.append("../")


def keep_latest_row_for_each_post_officer(post):
    duplicated_uids = set(post.loc[post.uid.duplicated(), 'uid'].to_list())
    post = post.set_index('uid', drop=False)
    level_1_cert_dates = post.loc[
        post.uid.isin(duplicated_uids) & (post.level_1_cert_date.notna()),
        'level_1_cert_date']
    for idx, value in level_1_cert_dates.iteritems():
        post.loc[idx, 'level_1_cert_date'] = value
    post = post.sort_values('last_pc_12_qualification_date', ascending=False)
    return post[~post.index.duplicated(keep='first')]


def prepare_post(agency):
    post = pd.read_csv(data_file_path("clean/pprr_post_2020_11_06.csv"))
    post = post[post.agency == agency]
    return keep_latest_row_for_each_post_officer(post)
