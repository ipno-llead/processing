import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_personnel_columns, rearrange_event_columns, rearrange_complaint_columns
)
from lib.uid import ensure_uid_unique
from lib import events
import sys
sys.path.append("../")


def fuse_events(pprr, cprr16, cprr18, cprr19):
    builder = events.Builder()
    builder.extract_events(pprr, {
        events.OFFICER_HIRE: {'prefix': 'hire'},
    }, ['uid'])
    builder.extract_events(cprr16, {
        events.COMPLAINT_RECEIVE: {'prefix': 'receive'},
        events.INVESTIGATION_COMPLETE: {'prefix': 'investigation_complete'},
        events.COMPLAINT_INCIDENT: {'prefix': 'occur'},
    }, ['uid', 'complaint_uid'])
    builder.extract_events(cprr18, {
        events.COMPLAINT_RECEIVE: {'prefix': 'receive'},
        events.COMPLAINT_INCIDENT: {'prefix': 'occur'},
    }, ['uid', 'complaint_uid'])
    builder.extract_events(cprr19, {
        events.COMPLAINT_RECEIVE: {'prefix': 'receive'},
        events.COMPLAINT_INCIDENT: {'prefix': 'occur'},
    }, ['uid', 'complaint_uid'])
    return builder.to_frame()


if __name__ == "__main__":
    cprr19 = pd.read_csv(
        data_file_path("match/cprr_port_allen_pd_2019.csv"))
    cprr18 = pd.read_csv(
        data_file_path("match/cprr_port_allen_pd_2017_2018.csv"))
    cprr16 = pd.read_csv(
        data_file_path("match/cprr_port_allen_pd_2015_2016.csv"))
    post_event = pd.read_csv(data_file_path(
        "match/post_event_port_allen_pd.csv"))
    pprr = pd.read_csv(data_file_path('match/pprr_port_allen_csd_2020.csv'))
    pprr.loc[:, 'agency'] = 'Port Allen PD'
    personnel_df = rearrange_personnel_columns(pprr)
    complaint_df = rearrange_complaint_columns(
        pd.concat([cprr16, cprr18, cprr19]))
    ensure_uid_unique(complaint_df, 'complaint_uid', True)
    events_df = fuse_events(pprr, cprr16, cprr18, cprr19)
    events_df = rearrange_event_columns(pd.concat([
        post_event,
        events_df
    ]))
    ensure_data_dir("fuse")
    personnel_df.to_csv(data_file_path(
        "fuse/per_port_allen_pd.csv"), index=False)
    events_df.to_csv(data_file_path(
        "fuse/event_port_allen_pd.csv"), index=False)
    complaint_df.to_csv(data_file_path(
        "fuse/com_port_allen_pd.csv"), index=False)
