from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_complaint_columns, rearrange_personnel_columns
)
from lib import events
import pandas as pd
import sys
sys.path.append('../')


def fuse_events(cprr, post):
    builder = events.Builder()
    builder.extract_events(cprr, {
        events.COMPLAINT_RECEIVE: {
            'prefix': 'receive', 'keep': ['uid', 'agency', 'complaint_uid']
        }
    })
    builder.extract_events(post, {
        events.OFFICER_LEVEL_1_CERT: {'prefix': 'level_1_cert', 'parse_date': '%Y-%m-%d', 'keep': [
            'uid', 'agency'
        ]},
        events.OFFICER_PC_12_QUALIFICATION: {'prefix': 'last_pc_12_qualification', 'parse_date': '%Y-%m-%d', 'keep': [
            'uid', 'agency'
        ]},
        events.OFFICER_HIRE: {'prefix': 'hire', 'keep': ['uid', 'agency']},
    })
    return builder.to_frame(["kind", "year", "month", "day", "uid", "complaint_uid"])


if __name__ == '__main__':
    cprr = pd.read_csv(data_file_path('match/cprr_plaquemines_so_2019.csv'))
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    post = post[post.agency == 'plaquemines par so']
    post.loc[:, 'agency'] = 'Plaquemines SO'
    event = fuse_events(cprr, post)
    ensure_data_dir('fuse')
    rearrange_complaint_columns(cprr).to_csv(
        data_file_path('fuse/com_plaquemines_so.csv'),
        index=False
    )
    rearrange_personnel_columns(post).to_csv(
        data_file_path('fuse/per_plaquemines_so.csv'),
        index=False
    )
    event.to_csv(
        data_file_path('fuse/event_plaquemines_so.csv'),
        index=False
    )
