from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_appeal_hearing_columns
)
from lib.post import keep_latest_row_for_each_post_officer
from lib.personnel import fuse_personnel
from lib import events
import pandas as pd
import sys
sys.path.append('../')


def fuse_events(lprr, post):
    builder = events.Builder()
    builder.extract_events(lprr, {
        events.APPEAL_FILE: {
            'prefix': 'filed', 'keep': ['uid', 'agency', 'appeal_uid']
        },
        events.APPEAL_RENDER: {
            'prefix': 'rendered', 'keep': ['uid', 'agency', 'appeal_uid']
        }
    }, ['uid', 'appeal_uid'])
    builder.extract_events(post, {
        events.OFFICER_LEVEL_1_CERT: {'prefix': 'level_1_cert', 'parse_date': '%Y-%m-%d', 'keep': [
            'uid', 'agency'
        ]},
        events.OFFICER_PC_12_QUALIFICATION: {'prefix': 'last_pc_12_qualification', 'parse_date': '%Y-%m-%d', 'keep': [
            'uid', 'agency'
        ]},
        events.OFFICER_HIRE: {'prefix': 'hire', 'keep': ['uid', 'agency']},
    }, ['uid'])
    return builder.to_frame()


if __name__ == '__main__':
    lprr = pd.read_csv(data_file_path(
        'match/lprr_louisiana_state_csc_1991_2020.csv'))
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    post = post[post.agency == 'la state police']
    post.loc[:, 'agency'] = 'Louisiana State Police'
    per_df = fuse_personnel(lprr, keep_latest_row_for_each_post_officer(post))
    event_df = fuse_events(lprr, post)
    ensure_data_dir('fuse')
    per_df.to_csv(data_file_path(
        'fuse/per_louisiana_state_police.csv'
    ), index=False)
    event_df.to_csv(data_file_path(
        'fuse/event_louisiana_state_police.csv'
    ), index=False)
    rearrange_appeal_hearing_columns(lprr).to_csv(data_file_path(
        'fuse/app_louisiana_state_police.csv'
    ), index=False)
