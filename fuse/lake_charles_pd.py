import sys
sys.path.append("../")
import pandas as pd
from lib.path import data_file_path
from lib import events
from lib.columns import rearrange_complaint_columns
from lib.personnel import fuse_personnel
from lib.uid import ensure_uid_unique


def prepare_post_data():
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    return post[post.agency == 'lake charles pd']


def fuse_events(cprr20, cprr19, post):
    builder = events.Builder()
    builder.extract_events(cprr20, {
        events.INVESTIGATION_START: {
            'prefix': 'investigation_start',
            'parse_date': True,
            'keep': ['uid', 'agency', 'complaint_uid'],
        },
    }, ['uid', 'complaint_uid'])
    builder.extract_events(cprr19, {
        events.INVESTIGATION_START: {
            'prefix': 'investigation_start',
            'parse_date': True,
            'keep': ['uid', 'agency', 'complaint_uid'],
        },
    }, ['uid', 'complaint_uid'])
    builder.extract_events(post, {
        events.OFFICER_LEVEL_1_CERT: {
            'prefix': 'level_1_cert',
            'parse_date': '%Y-%m-%d',
            'keep': ['uid', 'agency', 'employment_status']
        },
        events.OFFICER_HIRE: {
            'prefix': 'hire',
            'keep': ['uid', 'agency', 'employment_status']
        },
        events.OFFICER_PC_12_QUALIFICATION: {
            "prefix": 'last_pc_12_qualification',
            'parse_date': '%Y-%m-%d',
            'keep': ['uid', 'agency', 'employment_status']
        },
    }, ['uid'])
    return builder.to_frame()


if __name__ == '__main__':
    cprr20 = pd.read_csv(data_file_path('match/cprr_lake_charles_pd_2020.csv'))
    cprr19 = pd.read_csv(data_file_path('match/cprr_lake_charles_pd_2014_2019.csv'))
    post = prepare_post_data()
    per = fuse_personnel(cprr20, cprr19, post)
    com = rearrange_complaint_columns(pd.concat([cprr20, cprr19]))
    ensure_uid_unique(com, 'complaint_uid')
    event = fuse_events(cprr20, cprr19, post)
    ensure_uid_unique(event, 'event_uid')
    event.to_csv(
        data_file_path('fuse/event_lake_charles_pd.csv'), index=False)
    com.to_csv(
        data_file_path('fuse/com_lake_charles_pd.csv'), index=False)
    per.to_csv(
        data_file_path('fuse/per_lake_charles_pd.csv'), index=False)
