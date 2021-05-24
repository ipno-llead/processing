import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.columns import rearrange_complaint_columns, rearrange_personnel_columns
from lib.uid import ensure_uid_unique
from lib import events

import sys
sys.path.append("../")


def prepare_post():
    post_pprr = pd.read_csv(
        data_file_path('clean/pprr_post_2020_11_06.csv')
    )
    post_pprr = post_pprr.loc[
        (post_pprr.agency == 'scott pd')
    ]
    post_pprr.loc[:, 'data_production_year'] = '2020'
    post_pprr.loc[:, 'agency'] = 'Scott PD'
    return post_pprr


def fuse_events(cprr, post_pprr):
    builder = events.Builder()
    builder.extract_events(cprr, {
        events.COMPLAINT_RECEIVE: {
            'prefix': 'receive', 'keep': ['uid', 'agency', 'complaint_uid']
        },
    }, ['uid', 'complaint_uid'])
    builder.extract_events(post_pprr, {
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
    cprr = pd.read_csv(
        data_file_path("match/cprr_scott_pd_2020.csv"))
    post = prepare_post()
    per = rearrange_personnel_columns(post)
    com = rearrange_complaint_columns(cprr)
    ensure_uid_unique(com, 'complaint_uid')
    event = fuse_events(cprr, post)
    ensure_data_dir('fuse')
    per.to_csv(
        data_file_path('fuse/per_scott_pd.csv'),
        index=False
    )
    com.to_csv(
        data_file_path('fuse/com_scott_pd.csv'),
        index=False
    )
    event.to_csv(
        data_file_path('fuse/event_scott_pd.csv'),
        index=False
    )


