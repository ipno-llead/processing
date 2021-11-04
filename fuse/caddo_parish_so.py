import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.columns import rearrange_event_columns, rearrange_personnel_columns
from lib.uid import ensure_uid_unique
from lib import events
import sys
sys.path.append('../')


def fuse_events(pprr):
    builder = events.Builder()
    builder.extract_events(pprr, {
        events.OFFICER_HIRE: {
            'prefix': 'hire', 'keep': ['uid', 'agency', 'employee_id', 'rank_desc', 'salary', 'salary_freq']
        }
    }, ['uid'])
    return builder.to_frame()


if __name__ == '__main__':
    pprr = pd.read_csv(data_file_path('clean/pprr_caddo_parish_so_2020.csv'))
    post_event = pd.read_csv(data_file_path(
        'match/post_event_caddo_parish_so.csv'))
    cprr_post_event = pd.read_csv(data_file_path(
        'match/cprr_post_event_caddo_parish_so.csv'))
    event_df = fuse_events(pprr)
    event_df = rearrange_event_columns(pd.concat([
        post_event,
        event_df, 
        cprr_post_event
    ]))
    ensure_uid_unique(event_df, 'event_uid', True)
    ensure_data_dir('fuse')
    rearrange_personnel_columns(pprr).to_csv(
        data_file_path('fuse/per_caddo_parish_so.csv'), index=False)
    event_df.to_csv(data_file_path(
        'fuse/event_caddo_parish_so.csv'), index=False)
