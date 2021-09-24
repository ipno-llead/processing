from lib.path import data_file_path
from lib.columns import (
    rearrange_complaint_columns, rearrange_event_columns
)
from lib.personnel import fuse_personnel
from lib import events
import pandas as pd
import sys
sys.path.append('../')


def fuse_events(cprr, pprr):
    builder = events.Builder()
    builder.extract_events(cprr, {
        events.COMPLAINT_RECEIVE: {
            'prefix': 'receive', 'keep': ['uid', 'agency', 'complaint_uid']
        }
    }, ['uid', 'complaint_uid'])
    builder.extract_events(pprr, {
        events.OFFICER_HIRE: {
            'prefix': 'hire',
            'keep': ['uid', 'agency', 'department_desc', 'sub_department_desc']
        }
    }, ['uid'])
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
    events_df = rearrange_event_columns(pd.concat([
        fuse_events(cprr, pprr),
        post_event
    ]))
    fuse_personnel(pprr, cprr).to_csv(data_file_path(
        'fuse/per_plaquemines_so.csv'
    ), index=False)
    rearrange_complaint_columns(cprr).to_csv(data_file_path(
        'fuse/com_plaquemines_so.csv'
    ), index=False)
    events_df.to_csv(data_file_path(
        'fuse/event_plaquemines_so.csv'), index=False)
