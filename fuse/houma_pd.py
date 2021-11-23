import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path
from lib import events
from lib.personnel import fuse_personnel
from lib.columns import rearrange_complaint_columns


def prepare_post_data():
    post = pd.read_csv(data_file_path("clean/pprr_post_2020_11_06.csv"))
    return post[post.agency == 'houma pd']


def fuse_events(post):
    builder = events.Builder()
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
    cprr = pd.read_csv(data_file_path('match/cprr_houma_pd_2019_2021.csv'))
    post = prepare_post_data()
    personnel_df = fuse_personnel(cprr, post)
    complaints_df = rearrange_complaint_columns(cprr)
    event_df = fuse_events(post)
    event_df.to_csv(data_file_path('fuse/event_houma_pd.csv'))
    personnel_df.to_csv(data_file_path('fuse/per_houma_pd.csv'))
    complaints_df.to_csv(data_file_path('fuse/com_houma_pd.csv'))
