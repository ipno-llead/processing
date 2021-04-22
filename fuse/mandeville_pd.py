from lib import events
from lib.columns import (
    rearrange_complaint_columns, rearrange_event_columns)
from lib.path import data_file_path, ensure_data_dir
from lib.uid import ensure_uid_unique
from lib.personnel import fuse_personnel
import pandas as pd
import sys
sys.path.append('../')


def fuse_events(cprr, pprr):
    builder = events.Builder()
    builder.extract_events(cprr, {
        events.COMPLAINT_RECEIVE: {'prefix': 'receive', 'keep': ['uid', 'complaint_uid', 'agency']},
        events.COMPLAINT_INCIDENT: {
            'prefix': 'occur', 'keep': ['uid', 'complaint_uid', 'agency']},
        events.INVESTIGATION_COMPLETE: {
            'prefix': 'investigation_complete', 'keep': ['uid', 'complaint_uid', 'agency']}
    })
    builder.extract_events(pprr, {
        events.OFFICER_HIRE: {
            'prefix': 'hire', 'keep': ['uid', 'badge_no', 'rank_desc', 'annual_salary', 'agency']},
        events.OFFICER_LEFT: {
            'prefix': 'term', 'keep': ['uid', 'badge_no', 'rank_desc', 'annual_salary', 'agency']}
    })
    return builder.to_frame(['kind', 'year', 'month', 'day', 'uid', 'complaint_uid'])


if __name__ == '__main__':
    pprr = pd.read_csv(data_file_path('clean/pprr_mandeville_csd_2020.csv'))
    post_event = pd.read_csv(data_file_path(
        'match/post_event_mandeville_pd_2019.csv'))
    cprr = pd.read_csv(data_file_path('match/cprr_mandeville_pd_2019.csv'))
    events_df = fuse_events(cprr, pprr)
    events_df = rearrange_event_columns(pd.concat([
        post_event,
        events_df
    ]))
    ensure_uid_unique(events_df, 'event_uid', True)
    per_df = fuse_personnel(pprr, cprr)
    ensure_uid_unique(per_df, 'uid', True)
    com_df = rearrange_complaint_columns(cprr)
    ensure_uid_unique(com_df, 'complaint_uid', True)
    ensure_data_dir('fuse')
    events_df.to_csv(data_file_path(
        'fuse/event_mandeville_pd.csv'), index=False)
    com_df.to_csv(
        data_file_path('fuse/com_mandeville_pd.csv'), index=False)
    per_df.to_csv(
        data_file_path('fuse/per_mandeville_pd.csv'), index=False)
