import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path
from lib import events
from lib.personnel import fuse_personnel
from lib.columns import rearrange_allegation_columns


def prepare_post_data():
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    return post[post.agency == 'Hammond PD']


def fuse_events(cprr_20, cprr_14, cprr_08, post):
    builder = events.Builder()
    builder.extract_events(cprr_20, {
        events.INVESTIGATION_START: {
            'prefix': 'investigation_start',
            'keep': ['uid', 'agency', 'allegation_uid']
        },
        events.COMPLAINT_INCIDENT: {
            'prefix': 'incident',
            'keep': ['uid', 'agency', 'allegation_uid']
        },
    },
        ['uid', 'allegation_uid'])
    builder.extract_events(cprr_14, {
        events.INVESTIGATION_START: {
            'prefix': 'investigation_start',
            'keep': ['uid', 'agency', 'allegation_uid']
        },
    },
        ['uid', 'allegation_uid'])
    builder.extract_events(cprr_08, {
        events.COMPLAINT_INCIDENT: {
            'prefix': 'incident',
            'keep': ['uid', 'agency', 'allegation_uid']
        },
        events.INITIAL_ACTION: {
            'prefix': 'initial_action',
            'keep': ['uid', 'agency', 'allegation_uid']
        },
    },
        ['uid', 'allegation_uid'])
    builder.extract_events(post, {
        events.OFFICER_LEVEL_1_CERT: {
            'prefix': 'level_1_cert',
            'parse_date': '%Y-%m-%d',
            'keep': ['uid', 'agency', 'employement_status']
        },
        events.OFFICER_PC_12_QUALIFICATION: {
            'prefix': 'last_pc_12_qualification',
            'parse_date': '%Y-%m-%d',
            'keep': ['uid', 'agency', 'employment status']
        },
        events.OFFICER_HIRE: {
            'prefix': 'hire',
            'keep': ['uid', 'agency', 'employment_status']
        },
    }, ['uid'])
    return builder.to_frame()


if __name__ == '__main__':
    cprr_20 = pd.read_csv(data_file_path('match/cprr_hammond_pd_2015_2020.csv'))
    cprr_14 = pd.read_csv(data_file_path('match/cprr_hammond_pd_2009_2014.csv'))
    cprr_08 = pd.read_csv(data_file_path('clean/cprr_hammond_pd_2004_2008.csv'))
    post = prepare_post_data()
    personnel_df = fuse_personnel(cprr_20, cprr_14, cprr_08, post)
    complaints_df = rearrange_allegation_columns(pd.concat([cprr_20, cprr_14, cprr_08]))
    event_df = fuse_events(cprr_20, cprr_14, cprr_08, post)
    event_df.to_csv(data_file_path('fuse/event_hammond_pd.csv'))
    personnel_df.to_csv(data_file_path('fuse/per_hammond_pd.csv'))
    complaints_df.to_csv(data_file_path('fuse/com_hammond_pd.csv'))
