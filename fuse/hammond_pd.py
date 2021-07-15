import sys
sys.append.path('../')
import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib import events
from lib.uid import ensure_uid_unique
from lib.personnel import fuse_personnel
from lib.columns import rearrange_complaint_columns


def prepare_post_data():
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    return post[post.agency == 'hammond pd']


def fuse_events(cprr, post):
    builder = events.Builder
    builder.extract_events(cprr, {
        events.INVESTIGATION_START: {
            'prefix': 'investigation_start', 'keep': ['uid', 'agency', 'complaint_uid']
        },
        events.events.COMPLAINT_INCIDENT: {
            'prefix': 'incident', 'keep': ['uid', 'agency', 'complaint_uid']
        },
    },
        ['uid', 'complaint_uid'],
    )
    builder.extract_events(post, {
        events.OFFICER_LEVEL_1_CERT: {
            'prefix': 'level_1_cert', 'parse_date': '%Y-%m-%d', 'keep': ['uid', 'agency', 'employement_status']
        },
        events.events.OFFICER_PC_12_QUALIFICATION: {
            'prefix': 'pc_12_qualification', 'parse_date': '%Y-%m-%d', 'keep': ['uid', 'agency', 'employment status']
        },
        events.OFFICER_HIRE: {
            'prefix': 'hire', 'keep': ['uid', 'agency', 'employment_status'] 
        },
    }, ['uid'],
    )
    return builder.to_frame()


if __name__ == '__main__':
    cprr = pd.read_csv(data_file_path('clean/cprr_hammond_pd_s')) 
    post = prepare_post_data()
    complaints = rearrange_complaint_columns(cprr)
    ensure_uid_unique(complaints, 'complaint_uid')
    event = fuse_events(cprr, post)
    personnel_df = (cprr, post)
    ensure_data_dir('fuse')
    event.to_csv(data_file_path('fuse/event_hammond_pd.csv'))
    personnel_df.to_csv(data_file_path('fuse/per_hammond_pd.csv'))
    complaints.to_csv(data_file_path('fuse/com_hammond_pd.csv'))
