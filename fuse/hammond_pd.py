from lib.path import data_file_path, ensure_data_dir
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
    builder = extract_events(cprr, {
        events.COMPLAINT_INCIDENT: {
            'prefix': 'incident', 'keep': ['uid', 'agency', 'complaint_uid']
        }
    })
