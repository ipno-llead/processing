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
            'keep': ['agency', 'uid']
        },
        events.OFFICER_PAY_EFFECTIVE: {
            'prefix': 'salary',
            'keep': ['agency', 'uid', 'salary', 'salary_freq']
        }
    }, ['uid'])
    return builder.to_frame()


if __name__ == '__main__':
    pprr = pd.read_csv(data_file_path(
        'match/pprr_youngsville_pd_2017_2019.csv'
    ))
    post_event = pd.read_csv(data_file_path(
        'match/post_event_youngsville_pd_2020.csv'
    ))
    events_df = rearrange_event_columns(pd.concat([
        fuse_events(pprr),
        post_event,
    ]))
    ensure_uid_unique(events_df, 'event_uid')
    per_df = rearrange_personnel_columns(pprr.drop_duplicates())
    ensure_uid_unique(per_df, 'uid')
    per_df.to_csv(data_file_path(
        'fuse/per_youngsville_pd.csv'
    ), index=False)
    events_df.to_csv(data_file_path(
        'fuse/event_youngsville_pd.csv'
    ), index=False)
