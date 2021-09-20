from lib.personnel import fuse_personnel
from lib.path import data_file_path
from lib.columns import (
    rearrange_complaint_columns, rearrange_event_columns)
from lib import events
import pandas as pd
import sys
sys.path.append('../')


def fuse_events(cprr):
    builder = events.Builder()
    builder.extract_events(cprr, {
        events.COMPLAINT_RECEIVE: {
            'prefix': 'receive', 'keep': ['uid', 'agency', 'complaint_uid']
        }
    }, ['uid', 'complaint_uid'])
    return builder.to_frame()


if __name__ == '__main__':
    cprr = pd.read_csv(data_file_path(
        'match/cprr_plaquemines_so_2019.csv'
    ))
    pprr = pd.read_csv(data_file_path(
        'clean/pprr_plaquemines_so_2018.csv'
    ))
    post_event = pd.read_csv(data_file_path(
        'match/event_plaquemines_so_2018.csv'))
    per_df = fuse_personnel(pprr, cprr)
    complaint_df = rearrange_complaint_columns(cprr)
    events_df = fuse_events(cprr)
    events_df = rearrange_event_columns(pd.concat([
        post_event,
        events_df
    ]))
    per_df.to_csv(data_file_path(
        'fuse/per_plaquemines_so.csv'), index=False)
    events_df.to_csv(data_file_path(
        'fuse/event_plaquemines_so.csv'), index=False)
    complaint_df.to_csv(data_file_path(
        'fuse/com_plaquemines_so.csv'), index=False)
