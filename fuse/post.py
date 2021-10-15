import sys
sys.path.append('../')
import pandas as pd 
from lib.path import data_file_path
from lib import events
from lib.columns import rearrange_complaint_columns
from lib.personnel import fuse_personnel


def fuse_events(cprr):
    builder = events.Builder()
    builder.extract_events(cprr, {
        events.OFFICER_POST_DECERTIFICATION: {
            'prefix': 'decertification',
            'parse_date': True,
            'keep': ['uid', 'agency'],
        },
    }, ['uid'])
    return builder.to_frame()


if __name__ == '__main__':
    cprr = pd.read_csv(data_file_path('clean/cprr_post_2016_2019.csv'))
    com = rearrange_complaint_columns(cprr)
    per = fuse_personnel(cprr)
    event_df = fuse_events(cprr)
    com.to_csv(data_file_path(
        'fuse/com_post.csv'), index=False)
    per.to_csv(data_file_path(
        'fuse/per_post.csv'), index=False)
    event_df.to_csv(data_file_path(
        'fuse/event_post.csv'), index=False)
