import sys
sys.path.append('../')
import pandas as pd
from lib.personnel import fuse_personnel
from lib.columns import rearrange_complaint_columns, rearrange_event_columns
from lib.path import data_file_path
from lib import events


def fuse_events(pprr, cprr):
    builder = events.Builder()
    builder.extract_events(pprr, {
        events.OFFICER_HIRE: {
            'prefix': 'hire',
            'keep': ['uid', 'agency', 'rank_desc', 'salary', 'salary_freq']},
    }, ['uid'])
    builder.extract_events(cprr, {
        events.COMPLAINT_RECEIVE: {
            'prefix': 'receive',
            'keep': ['uid', 'charges', 'disposition', 'action']
        },
        events.INVESTIGATION_START: {
            'prefix': 'investigation_start',
            'keep': ['uid', 'charges', 'disposition', 'action']
        },
        events.INVESTIGATION_COMPLETE: {
            'prefix': 'investigation_complete',
            'keep': ['uid', 'charges', 'disposition', 'action']
        }
    }, ['uid', 'complaint_uid'])


if __name__ == '__main__':
    pprr = pd.read_csv(data_file_path('clean/pprr_bossier_city_pd_2000_2019.csv'))
    post_event = pd.read_csv(data_file_path('match/post_event_bossier_city_pd.csv'))
    cprr = pd.read_csv(data_file_path('clean/cprr_bossier_city_pd_2020.csv'))
    per_df = fuse_personnel(pprr, cprr)
    events_df = fuse_events(pprr, cprr)
    com_df = rearrange_complaint_columns(cprr)
    events_df = rearrange_event_columns(pd.concat([
        post_event,
        events_df
    ]))
    per_df.to_csv(data_file_path(
        'fuse/per_bossier_city_pd.csv'), index=False)
    events_df.to_csv(data_file_path(
        'fuse/event_bossier_city_pd.csv'), index=False)
    com_df.to_csv(data_file_path(
        'fuse/com_bossier_city_pd.csv'), index=False)
