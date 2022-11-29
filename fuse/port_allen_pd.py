import pandas as pd
import deba
from lib.columns import (
    rearrange_personnel_columns,
    rearrange_event_columns,
    rearrange_allegation_columns,
)
from lib import events
from lib.personnel import fuse_personnel
from lib.post import load_for_agency


def fuse_events(pprr, cprr16, cprr18, cprr19):
    builder = events.Builder()
    builder.extract_events(
        pprr,
        {
            events.OFFICER_HIRE: {"prefix": "hire"},
        },
        ["uid"],
    )
    builder.extract_events(
        cprr16,
        {
            events.COMPLAINT_RECEIVE: {"prefix": "receive"},
            events.INVESTIGATION_COMPLETE: {"prefix": "investigation_complete"},
            events.COMPLAINT_INCIDENT: {"prefix": "occur"},
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr18,
        {
            events.COMPLAINT_RECEIVE: {"prefix": "receive"},
            events.COMPLAINT_INCIDENT: {"prefix": "occur"},
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr19,
        {
            events.COMPLAINT_RECEIVE: {"prefix": "receive"},
            events.COMPLAINT_INCIDENT: {"prefix": "occur"},
        },
        ["uid", "allegation_uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr19 = pd.read_csv(deba.data("match/cprr_port_allen_pd_2019.csv"))
    cprr18 = pd.read_csv(deba.data("match/cprr_port_allen_pd_2017_2018.csv"))
    cprr16 = pd.read_csv(deba.data("match/cprr_port_allen_pd_2015_2016.csv"))
    post_event = pd.read_csv(deba.data("match/post_event_port_allen_pd.csv"))
    pprr = pd.read_csv(deba.data("match/pprr_port_allen_csd_2020.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    pprr.loc[:, "agency"] = "port-allen-pd"
    personnel_df = rearrange_personnel_columns(pprr)
    personnel_df = fuse_personnel(personnel_df, post)
    complaint_df = rearrange_allegation_columns(pd.concat([cprr16, cprr18, cprr19], axis=0))
    events_df = fuse_events(pprr, cprr16, cprr18, cprr19)
    events_df = rearrange_event_columns(pd.concat([post_event, events_df]))
    personnel_df.to_csv(deba.data("fuse/per_port_allen_pd.csv"), index=False)
    events_df.to_csv(deba.data("fuse/event_port_allen_pd.csv"), index=False)
    complaint_df.to_csv(deba.data("fuse/com_port_allen_pd.csv"), index=False)
    post.to_csv(deba.data("fuse/post_port_allen_pd.csv"), index=False)
