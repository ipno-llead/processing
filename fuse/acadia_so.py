import sys
sys.path.append('../')
from lib.path import data_file_path
from lib.columns import rearrange_allegation_columns
from lib.personnel import fuse_personnel
import pandas as pd
from lib import events


def prepare_post_data():
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    return post[post.agency == 'acadia parish so']


def fuse_events(cprr, post):
    builder = events.Builder()
    builder.extract_events(cprr, {
        events.COMPLAINT_RECEIVE: {'prefix': 'receive', 'keep': ['uid', 'agency', 'allegation', 'action']}
    }, ['uid', 'allegation_uid'])
    builder.extract_events(post, {
        events.OFFICER_LEVEL_1_CERT: {'prefix': 'level_1_cert', 'parse_date': '%Y-%m-%d', 'keep': [
            'uid', 'agency'
        ]},
        events.OFFICER_PC_12_QUALIFICATION: {'prefix': 'last_pc_12_qualification', 'parse_date': '%Y-%m-%d', 'keep': [
            'uid', 'agency'
        ]},
        events.OFFICER_HIRE: {'prefix': 'hire', 'keep': ['uid', 'agency']},
    }, ['uid'])
    return builder.to_frame()


if __name__ == '__main__':
    cprr = pd.read_csv(data_file_path(
        'clean/cprr_acadia_so_2018_2021.csv'))
    post = prepare_post_data()
    per = fuse_personnel(cprr, post)
    complaints = rearrange_allegation_columns(cprr)
    event = fuse_events(cprr, post)
    complaints.to_csv(data_file_path('fuse/com_acadia_so.csv'), index=False)
    per.to_csv(data_file_path('fuse/per_acadia_so.csv'), index=False)
    event.to_csv(data_file_path('fuse/event_acadia_so.csv'), index=False)
