from lib import events
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


def extract_events_from_post(post, uid_matches):
    builder = events.Builder()
    for pprr_uid, post_uid in uid_matches:
        for _, row in post[post.uid == post_uid].iterrows():
            if pd.notnull(row.level_1_cert_date):
                builder.append(
                    events.OFFICER_LEVEL_1_CERT,
                    raw_date_str=row.level_1_cert_date,
                    strptime_format='%Y-%m-%d',
                    uid=pprr_uid)
            if pd.notnull(row.last_pc_12_qualification_date):
                builder.append(
                    events.OFFICER_PC_12_QUALIFICATION,
                    raw_date_str=row.last_pc_12_qualification_date,
                    strptime_format='%Y-%m-%d',
                    uid=pprr_uid)
    return builder.to_frame(['kind', 'uid', 'year', 'month', 'day'])
