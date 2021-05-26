from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_complaint_columns, rearrange_event_columns
)
from lib.personnel import fuse_personnel
from lib import events
from lib.uid import ensure_uid_unique
import pandas as pd
import sys
sys.path.append('../')


def fuse_events(pprr, cprr):
    builder = events.Builder()
    builder.extract_events(pprr, {
        events.OFFICER_HIRE: {
            'prefix': 'hire', 'keep': ['uid', 'badge_no' 'agency', 'rank_desc', 'salary', 'salary_freq']
        },
        events.OFFICER_LEFT: {
            'prefix': 'term', 'keep': ['uid', 'badge_no', 'agency', 'rank_desc', 'salary', 'salary_freq']
        }
    }, ['uid'])
    builder.extract_events(cprr, {
        events.COMPLAINT_INCIDENT: {
            'prefix': 'receive', 'keep': ['uid', 'agency', 'complaint_uid']
        }
    }, ['uid', 'complaint_uid'])
    return builder.to_frame()


if __name__ == '__main__':
    cprr = pd.read_csv(data_file_path(
        'match/cprr_scott_pd_2021.csv'
    ))
    pprr = pd.read_csv(data_file_path(
        'clean/pprr_scott_pd_2021.csv'
    ))
    post_event = pd.read_csv(data_file_path(
        'match/post_event_scott_pd_2021.csv'))
    personnels = fuse_personnel(pprr, cprr)
    complaints = rearrange_complaint_columns(cprr)
    ensure_uid_unique(complaints, 'complaint_uid', True)
    events_df = fuse_events(pprr, cprr)
    events_df = rearrange_event_columns(pd.concat([
        post_event,
        events_df
    ]))
    ensure_uid_unique(events_df, 'event_uid', True)
    ensure_data_dir('fuse')
    personnels.to_csv(data_file_path(
        'fuse/per_scott_pd.csv'), index=False)
    events_df.to_csv(data_file_path(
        'fuse/event_scott_pd.csv'), index=False)
    complaints.to_csv(data_file_path(
        'fuse/com_scott_pd.csv'), index=False)
