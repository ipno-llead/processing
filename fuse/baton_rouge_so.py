import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.columns import rearrange_complaint_columns
from lib.personnel import fuse_personnel
from lib import events

import sys
sys.path.append("../")


def prepare_post_data():
    post = pd.read_csv(data_file_path("clean/pprr_post_2020_11_06.csv"))
    post = post[post.agency == 'e. baton rouge so']
    post.loc[:, 'agency'] = 'Baton Rouge SO'
    return post


def fuse_events(cprr, post):
    builder = events.Builder()
    builder.extract_events(cprr, {
        events.OFFICER_RANK: {
            'prefix': 'rank', 'keep': ['uid', 'agency', 'badge_no', 'rank_desc'], 'id_cols': ['uid']},
        events.COMPLAINT_INCIDENT: {'prefix': 'occur', 'keep': ['uid', 'agency', 'complaint_uid']},
    }, ['uid', 'complaint_uid'])
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


if __name__ == "__main__":
    cprr = pd.read_csv(
        data_file_path("match/cprr_baton_rouge_so_2018.csv"))
    post = prepare_post_data()
    personnel_df = fuse_personnel(cprr, post)
    event_df = fuse_events(cprr, post)
    complaint_df = rearrange_complaint_columns(cprr)
    ensure_data_dir("fuse")
    personnel_df.to_csv(data_file_path(
        "fuse/per_baton_rouge_so.csv"), index=False)
    event_df.to_csv(data_file_path(
        "fuse/event_baton_rouge_so.csv"), index=False)
    complaint_df.to_csv(data_file_path(
        "fuse/com_baton_rouge_so.csv"), index=False)
