import sys

import pandas as pd

from lib.path import data_file_path, ensure_data_dir
from lib.columns import rearrange_event_columns
from lib.personnel import fuse_personnel
from lib.uid import ensure_uid_unique
from lib import events

sys.path.append('../')


def fuse_events(pdpprr, csdpprr):
    builder = events.Builder()
    builder.extract_events(csdpprr, {
        events.OFFICER_HIRE: {
            'prefix': 'hire', 'keep': [
                'uid', 'agency', 'employee_id', 'salary', 'salary_freq', 'employment_status', 'rank_desc'
            ]
        },
        events.OFFICER_LEFT: {
            'prefix': 'left', 'parse_date': True, 'keep': [
                'uid', 'agency', 'employee_id', 'salary', 'salary_freq', 'employment_status', 'rank_desc'
            ]
        }
    }, ['uid'])
    builder.extract_events(pdpprr, {
        events.OFFICER_RANK: {
            'prefix': 'rank', 'keep': ['uid', 'agency', 'rank_desc', 'badge_no']
        }
    }, ['uid'])
    return builder.to_frame()


if __name__ == '__main__':
    pdpprr = pd.read_csv(data_file_path('match/pprr_harahan_pd_2020.csv'))
    csdpprr = pd.read_csv(data_file_path('clean/pprr_harahan_csd_2020.csv'))
    post_event = pd.read_csv(data_file_path(
        'match/post_event_harahan_pd.csv'
    ))
    events_df = fuse_events(pdpprr, csdpprr)
    events_df = rearrange_event_columns(pd.concat([
        post_event,
        events_df
    ]))
    ensure_uid_unique(events_df, 'event_uid', True)
    per = fuse_personnel(csdpprr, pdpprr)
    ensure_data_dir('fuse')
    events_df.to_csv(
        data_file_path('fuse/event_harahan_pd.csv'), index=False
    )
    per.to_csv(
        data_file_path('fuse/per_harahan_pd.csv'), index=False
    )
