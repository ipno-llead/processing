import sys

import pandas as pd

from lib.path import data_file_path, ensure_data_dir
from lib.columns import rearrange_complaint_columns
from lib.personnel import fuse_personnel
from lib.clean import float_to_int_str
from lib import events

sys.path.append('../')


def fuse_events(cprr, post):
    builder = events.Builder()
    builder.extract_events(cprr, {
        events.COMPLAINT_RECEIVE: {
            'prefix': 'receive', 'parse_date': '%Y', 'keep': ['uid', 'agency', 'complaint_uid'],
        },
    }, ['uid', 'complaint_uid'])
    builder.extract_events(post, {
        events.OFFICER_LEVEL_1_CERT: {'prefix': 'level_1_cert', 'parse_date': '%Y-%m-%d', 'keep': [
            'uid', 'agency'
        ]},
        events.OFFICER_PC_12_QUALIFICATION: {'prefix': 'last_pc_12_qualification', 'parse_date': '%Y-%m-%d', 'keep': [
            'uid', 'agency'
        ]},
        events.OFFICER_HIRE: {
            'prefix': 'hire', 'keep': ['uid', 'agency']
        }
    }, ['uid'])
    return builder.to_frame()


if __name__ == '__main__':
    post = pd.read_csv(data_file_path('match/post_event_new_orleans_pd_2021.csv'))
    cprr = pd.read_csv(data_file_path('match/cprr_new_orleans_da_2021.csv'))
    cprr = float_to_int_str(
        cprr, ['receive_date'])
    per = fuse_personnel(post, cprr)
    event = fuse_events(cprr, post)
    com = rearrange_complaint_columns(cprr)
    ensure_data_dir('fuse')
    per.to_csv(data_file_path('fuse/per_new_orleans_da.csv'), index=False)
    event.to_csv(data_file_path('fuse/event_new_orleans_da.csv'), index=False)
    com.to_csv(data_file_path('fuse/com_new_orleans_da.csv'), index=False)
