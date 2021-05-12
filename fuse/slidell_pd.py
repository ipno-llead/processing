import sys

import pandas as pd

from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_personnel_columns, rearrange_event_columns
)
from lib.uid import ensure_uid_unique
from lib import events

sys.path.append('../')


def fuse_events(pprr):
    builder = events.Builder()
    builder.extract_events(pprr, {
        events.OFFICER_HIRE: {
            'prefix': 'hire', 'keep': ['uid', 'agency', 'department_desc', 'badge_no', 'employment_status']
        },
        events.OFFICER_PAY_EFFECTIVE: {
            'prefix': 'salary', 'keep': ['uid', 'agency', 'rank_desc', 'salary', 'salary_freq']
        }
    }, ['uid'])
    return builder.to_frame()


if __name__ == '__main__':
    pprr = pd.read_csv(data_file_path('clean/pprr_slidell_pd_2019.csv'))
    post_event = pd.read_csv(data_file_path(
        'match/post_event_slidell_pd_2020.csv'
    ))
    events_df = rearrange_event_columns(pd.concat([
        post_event,
        fuse_events(pprr)
    ]))
    ensure_uid_unique(events_df, 'event_uid', True)
    ensure_data_dir('fuse')
    rearrange_personnel_columns(pprr).to_csv(data_file_path(
        "fuse/per_slidell_pd.csv"), index=False)
    events_df.to_csv(data_file_path(
        "fuse/event_slidell_pd.csv"), index=False)
