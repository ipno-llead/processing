import sys

import pandas as pd

from lib.path import data_file_path
from lib.columns import (
    rearrange_personnel_columns, rearrange_event_columns
)
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
    per_df = rearrange_personnel_columns(pprr.drop_duplicates())
    per_df.to_csv(data_file_path(
        'fuse/per_youngsville_pd.csv'
    ), index=False)
    events_df.to_csv(data_file_path(
        'fuse/event_youngsville_pd.csv'
    ), index=False)
