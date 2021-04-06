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
