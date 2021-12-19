import sys
sys.path.append("../")
import pandas as pd
from lib.path import data_file_path
from lib import events
from lib.columns import rearrange_allegation_columns
from lib.personnel import fuse_personnel


def prepare_post_data():
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    return post[post.agency == 'Cameron SO']


def fuse_events(post):
    builder = events.Builder()
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
    cprr = pd.read_csv(data_file_path('clean/cprr_cameron_so_2020.csv'))
    post = prepare_post_data()
    per = fuse_personnel(cprr, post)
    com = rearrange_allegation_columns(cprr)
    event = fuse_events(post)
    event.to_csv(
        data_file_path('fuse/event_cameron_so.csv'), index=False)
    com.to_csv(
        data_file_path('fuse/com_cameron_so.csv'), index=False)
    per.to_csv(
        data_file_path('fuse/per_cameron_so.csv'), index=False)
