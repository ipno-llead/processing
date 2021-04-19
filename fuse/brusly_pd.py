import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_personnel_columns, rearrange_complaint_columns, rearrange_event_columns
)
from lib.uid import gen_uid, ensure_uid_unique
from lib import events

import sys
sys.path.append("../")


def fuse_events(pprr, cprr):
    builder = events.Builder()
    builder.extract_events(pprr, {
        events.OFFICER_HIRE: {'prefix': 'hire', 'keep': ['uid', 'agency', 'rank_desc', 'annual_salary']},
        events.OFFICER_LEFT: {'prefix': 'term', 'keep': ['uid', 'agency', 'rank_desc', 'annual_salary']},
    })
    builder.extract_events(cprr, {
        events.COMPLAINT_RECEIVE: {'prefix': 'receive', 'keep': ['uid', 'agency', 'complaint_uid']},
        events.COMPLAINT_INCIDENT: {'prefix': 'occur', 'keep': ['uid', 'agency', 'complaint_uid']},
        events.SUSPENSION_START: {'prefix': 'suspension_start', 'keep': ['uid', 'agency', 'complaint_uid']},
        events.SUSPENSION_END: {'prefix': 'suspension_end', 'keep': ['uid', 'agency', 'complaint_uid']},
    })
    return builder.to_frame(['kind', 'year', 'month', 'day', 'uid', 'complaint_uid'])


if __name__ == "__main__":
    pprr = pd.read_csv(
        data_file_path("clean/pprr_brusly_pd_2020.csv"))
    post_event = pd.read_csv(data_file_path(
        "match/post_event_brusly_pd_2020.csv"))
    cprr = pd.read_csv(data_file_path("match/cprr_brusly_pd_2020.csv"))
    cprr = gen_uid(cprr, [
        'uid', 'occur_year', 'occur_month', 'occur_day'], 'complaint_uid')
    events_df = fuse_events(pprr, cprr)
    events_df = rearrange_event_columns(pd.concat([
        post_event,
        events_df
    ]))
    ensure_uid_unique(events_df, 'event_uid', True)
    com_df = rearrange_complaint_columns(cprr)
    ensure_data_dir("fuse")
    rearrange_personnel_columns(pprr).to_csv(data_file_path(
        "fuse/per_brusly_pd.csv"), index=False)
    events_df.to_csv(data_file_path(
        "fuse/event_brusly_pd.csv"), index=False)
    com_df.to_csv(data_file_path(
        "fuse/com_brusly_pd.csv"
    ), index=False)
