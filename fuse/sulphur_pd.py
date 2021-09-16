import sys

import pandas as pd

from lib.path import data_file_path
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
            'prefix': 'hire',
            'parse_date': True,
            'keep': ['agency', 'uid', 'employee_id', 'salary', 'salary_freq']
        },
        events.OFFICER_LEFT: {
            'prefix': 'termination',
            'parse_date': True,
            'keep': ['agency', 'uid', 'employee_id', 'salary', 'salary_freq']
        }
    }, ['uid'])
    return builder.to_frame()


if __name__ == '__main__':
    pprr = pd.read_csv(data_file_path(
        'clean/pprr_sulphur_pd_2021.csv'
    ))
    post_event = pd.read_csv(data_file_path(
        'match/post_event_sulphur_pd_2021.csv'
    ))
    events_df = rearrange_event_columns(pd.concat([
        post_event,
        fuse_events(pprr)
    ]))
    ensure_uid_unique(events_df, 'event_uid', True)
    per_df = rearrange_personnel_columns(pprr)
    per_df.to_csv(data_file_path(
        'fuse/per_sulphur_pd.csv'
    ), index=False)
    events_df.to_csv(data_file_path(
        'fuse/event_sulphur_pd.csv'
    ), index=False)
