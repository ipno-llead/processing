import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path
from lib.personnel import fuse_personnel
from lib import events


def fuse_events(post):
    builder = events.Builder()
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
    post = pd.read_csv(data_file_path(
        'clean/pprr_post_2020_11_06.csv'
    ))
    event_df = fuse_events(post)
    per = fuse_personnel(post)
    per.to_csv(data_file_path(
        'fuse/per_post.csv'
    ), index=False)
    event_df.to_csv(data_file_path(
        'fuse/event_post.csv'
    ), index=False)
