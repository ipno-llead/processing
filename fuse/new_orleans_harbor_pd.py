import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_personnel_columns, rearrange_event_columns,
    rearrange_complaint_columns
)
from lib import events
from lib.uid import ensure_uid_unique
import sys
sys.path.append("../")


def fuse_events(pprr08, pprr20, cprr):
    builder = events.Builder()
    builder.extract_events(pprr08, {
        events.OFFICER_HIRE: {'prefix': 'hire', 'keep': ['uid', 'agency']},
        events.OFFICER_LEFT: {'prefix': 'resign', 'keep': ['uid', 'agency']},
        events.OFFICER_PAY_EFFECTIVE: {'prefix': 'pay_effective', 'keep': ['uid', 'agency', 'hourly_salary']},
        events.OFFICER_RANK: {'prefix': 'rank', 'keep': ['uid', 'agency', 'rank_desc']},
    }, ['uid'])
    builder.extract_events(pprr20, {
        events.OFFICER_HIRE: {'prefix': 'hire', 'keep': ['uid', 'agency']},
        events.OFFICER_LEFT: {'prefix': 'resign', 'keep': ['uid', 'agency']},
        events.OFFICER_PAY_EFFECTIVE: {'prefix': 'pay_effective', 'keep': ['uid', 'agency', 'hourly_salary']},
        events.OFFICER_RANK: {'prefix': 'rank', 'keep': ['uid', 'agency', 'rank_desc']},
    }, ['uid'])
    builder.extract_events(cprr, {
        events.OFFICER_HIRE: {
            'prefix': 'hire', 'keep': ['uid', 'agency'], 'id_cols': ['uid']
        },
        events.COMPLAINT_INCIDENT: {
            'prefix': 'occur', 'keep': ['uid', 'agency', 'complaint_uid']
        },
        events.COMPLAINT_RECEIVE: {
            'prefix': 'receive', 'keep': ['uid', 'agency', 'complaint_uid']
        },
        events.INVESTIGATION_COMPLETE: {
            'prefix': 'investigation_complete', 'keep': ['uid', 'agency', 'complaint_uid']
        }
    }, ['uid', 'complaint_uid'])
    return builder.to_frame()


if __name__ == "__main__":
    pprr20 = pd.read_csv(
        data_file_path("clean/pprr_new_orleans_harbor_pd_2020.csv")
    )
    pprr08 = pd.read_csv(
        data_file_path("clean/pprr_new_orleans_harbor_pd_1991_2008.csv")
    )
    cprr = pd.read_csv(
        data_file_path("match/cprr_new_orleans_harbor_pd_2020.csv")
    )
    post_event = pd.read_csv(data_file_path(
        'match/post_event_new_orleans_harbor_pd_2020.csv'))
    personnel_df = rearrange_personnel_columns(pd.concat([pprr08, pprr20]))
    complaint_df = rearrange_complaint_columns(cprr)
    event_df = fuse_events(pprr08, pprr20, cprr)
    event_df = rearrange_event_columns(pd.concat([
        post_event,
        event_df
    ]))
    ensure_uid_unique(event_df, 'event_uid', True)
    ensure_data_dir("fuse")
    personnel_df.to_csv(data_file_path(
        "fuse/per_new_orleans_harbor_pd.csv"), index=False)
    event_df.to_csv(data_file_path(
        "fuse/event_new_orleans_harbor_pd.csv"), index=False)
    complaint_df.to_csv(data_file_path(
        "fuse/com_new_orleans_harbor_pd.csv"), index=False)
