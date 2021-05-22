import sys

import pandas as pd

from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_personnel_columns, rearrange_complaint_columns, rearrange_event_columns
)
from lib.uid import ensure_uid_unique
from lib import events

sys.path.append('../')


def fuse_events():
    builder = events.Builder()
    return builder.to_frame()


if __name__ == '__main__':
    ensure_data_dir('fuse')
