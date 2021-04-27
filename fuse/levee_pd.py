from lib.columns import rearrange_complaint_columns
from lib.personnel import fuse_personnel
from lib.path import data_file_path, ensure_data_dir
from lib.uid import ensure_uid_unique
from lib import events
import pandas as pd
import sys
sys.path.append('../')


def fuse_events(cprr, post):
    builder = events.Builder()
    builder.extract_events(cprr, {
        events.COMPLAINT_INCIDENT: {
            'prefix': 'occur', 'parse_date': True, 'keep': ['agency', 'uid', 'complaint_uid']
        },
        events.COMPLAINT_RECEIVE: {
            'prefix': 'receive', 'parse_date': True, 'keep': ['agency', 'uid', 'complaint_uid']
        },
        events.INVESTIGATION_COMPLETE: {
            'prefix': 'investigation_complete', 'parse_date': True, 'keep': ['agency', 'uid', 'complaint_uid']
        },
        events.INVESTIGATION_START: {
            'prefix': 'investigation_start', 'parse_date': True, 'keep': ['agency', 'uid', 'complaint_uid']
        },
    }, ['uid', 'complaint_uid'])
    builder.extract_events(post, {
        events.OFFICER_HIRE: {
            'prefix': 'hire', 'keep': ['agency', 'uid', 'employment_status'],
        },
        events.OFFICER_LEVEL_1_CERT: {
            'prefix': 'level_1_cert', 'parse_date': '%Y-%m-%d', 'keep': ['agency', 'uid'],
        },
        events.OFFICER_PC_12_QUALIFICATION: {
            'prefix': 'last_pc_12_qualification', 'parse_date': '%Y-%m-%d', 'keep': ['agency', 'uid'],
        },
    }, ['uid'])
    return builder.to_frame()


if __name__ == '__main__':
    cprr = pd.read_csv(data_file_path('match/cprr_levee_pd.csv'))
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    post = post.loc[
        (post.agency == 'e. jefferson levee pd') | (
            post.agency == 'orleans levee pd')
    ]
    event_df = fuse_events(cprr, post)
    agency_dict = {
        'e. jefferson levee pd': 'East Jefferson Levee PD',
        'orleans levee pd': 'Orleans Levee PD'
    }
    event_df.loc[:, 'agency'] = event_df.agency.map(
        lambda x: agency_dict.get(x, x))
    ensure_data_dir('fuse')
    event_df.to_csv(data_file_path('fuse/event_levee_pd.csv'), index=False)
    complaint_df = rearrange_complaint_columns(cprr)
    ensure_uid_unique(complaint_df, 'complaint_uid')
    complaint_df.to_csv(
        data_file_path('fuse/com_levee_pd.csv'), index=False)
    fuse_personnel(post, cprr).to_csv(
        data_file_path('fuse/per_levee_pd.csv'), index=False)
