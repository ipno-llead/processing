import sys
sys.path.append('../')
from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_complaint_columns, rearrange_event_columns
)
from lib import events
from lib.uid import ensure_uid_unique
import pandas as pd


def extract_events(cprr):
    builder = events.Builder()
    builder.extract_events(cprr, {
        events.INVESTIGATION_COMPLETE: {
            'prefix': 'completion', 'keep': ['uid', 'agency', 'complaint_uid']
        }
    }, ['uid', 'complaint_uid'])
    return builder.to_frame()


if __name__ == '__main__':
    cprr = pd.read_csv(data_file_path(
        'match/cprr_tangipahoa_so_2015_2021.csv'))
    post_event = pd.read_csv(data_file_path(
        'match/post_event_tangipahoa_so_2015_2021.csv'))
    complaints = rearrange_complaint_columns(cprr)
    ensure_uid_unique(complaints, 'complaint_uid', True)
    events_df = extract_events(cprr)
    events_df = rearrange_event_columns(pd.concat([
        post_event,
        events_df
    ]))
    ensure_uid_unique(events_df, 'event_uid', True)
    ensure_data_dir('fuse')
    events_df.to_csv(data_file_path(
        'fuse/event_tangipahoa_so.csv'), index=False)
    complaints.to_csv(data_file_path(
        'fuse/com_tangipahoa_so.csv'), index=False)
