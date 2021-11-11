from lib.personnel import fuse_personnel
import sys

import pandas as pd

from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_complaint_columns, rearrange_event_columns
)
from lib.uid import ensure_uid_unique
from lib import events

sys.path.append('../')


def fuse_events(cprr_20, cprr_14, pprr):
    builder = events.Builder()
    builder.extract_events(cprr_20, {
        events.COMPLAINT_RECEIVE: {
            'prefix': 'receive',
            'parse_date': True,
            'keep': ['agency', 'complaint_uid', 'uid', 'investigator_uid']
        },
        events.INVESTIGATION_COMPLETE: {
            'prefix': 'complete',
            'parse_date': True,
            'ignore_bad_date': True,
            'keep': ['agency', 'complaint_uid', 'uid', 'investigator_uid'],
        },
    }, ['complaint_uid'])
    builder.extract_events(cprr_14, {
        events.COMPLAINT_RECEIVE: {
            'prefix': 'receive',
            'keep': ['agency', 'complaint_uid', 'uid', 'investigator_uid']
        },
        events.INVESTIGATION_COMPLETE: {
            'prefix': 'complete',
            'keep': ['agency', 'complaint_uid', 'uid', 'investigator_uid'],
        },
    }, ['complaint_uid'])
    builder.extract_events(pprr, {
        events.OFFICER_HIRE: {
            'prefix': 'hire',
            'parse_date': True,
            'keep': ['agency', 'uid', 'salary', 'salary_freq', 'rank_desc']
        },
        events.OFFICER_LEFT: {
            'prefix': 'left',
            'parse_date': True,
            'keep': ['agency', 'uid', 'salary', 'salary_freq', 'rank_desc']
        }
    }, ['uid'])
    return builder.to_frame()


if __name__ == '__main__':
    cprr_20 = pd.read_csv(data_file_path(
        'match/cprr_lafayette_pd_2015_2020.csv'
    ))
    cprr_14 = pd.read_csv(data_file_path(
        'match/cprr_lafayette_pd_2009_2014.csv'
    ))
    pprr = pd.read_csv(data_file_path(
        'clean/pprr_lafayette_pd_2010_2021.csv'
    ))
    post_events = pd.read_csv(data_file_path(
        'match/post_event_lafayette_pd_2020.csv'
    ))
    events_df = fuse_events(cprr_20, cprr_14, pprr)
    events_df = rearrange_event_columns(pd.concat([
        post_events,
        events_df
    ]))
    ensure_uid_unique(events_df, 'event_uid', True)
    per = fuse_personnel(
        pprr,
        cprr_20[['uid', 'first_name', 'last_name']],
        cprr_20[['investigator_uid', 'investigator_first_name', 'investigator_last_name']].rename(columns={
            'investigator_uid': 'uid',
            'investigator_first_name': 'first_name',
            'investigator_last_name': 'last_name',
        }),
        cprr_14[['uid', 'first_name', 'last_name']],
        cprr_14[['investigator_uid', 'investigator_first_name', 'investigator_last_name']].rename(columns={
            'investigator_uid': 'uid',
            'investigator_first_name': 'first_name',
            'investigator_last_name': 'last_name',
        })
    )
    com = rearrange_complaint_columns(pd.concat([cprr_20, cprr_14]))

    ensure_data_dir('fuse')
    per.to_csv(data_file_path(
        'fuse/per_lafayette_pd.csv'
    ), index=False)
    com.to_csv(data_file_path(
        'fuse/com_lafayette_pd.csv'
    ), index=False)
    events_df.to_csv(data_file_path(
        'fuse/event_lafayette_pd.csv'
    ), index=False)
