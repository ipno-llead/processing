import pandas as pd

import bolo
from lib.columns import rearrange_allegation_columns, rearrange_event_columns
from lib.personnel import fuse_personnel
from lib import events


def fuse_events(cprr19, cprr20, pprr):
    builder = events.Builder()
    builder.extract_events(
        cprr19,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.INVESTIGATION_START: {
                "prefix": "investigation_start",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.INVESTIGATION_COMPLETE: {
                "prefix": "investigation_complete",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.SUSPENSION_START: {
                "prefix": "suspension_start",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.SUSPENSION_END: {
                "prefix": "suspension_end",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.OFFICER_LEFT: {
                "prefix": "resignation",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid", "left_reason"],
            },
            events.OFFICER_LEFT: {
                "prefix": "arrest",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid", "left_reason"],
            },
            events.OFFICER_LEFT: {
                "prefix": "termination",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid", "left_reason"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr20,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.INVESTIGATION_START: {
                "prefix": "investigation_start",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.INVESTIGATION_COMPLETE: {
                "prefix": "investigation_complete",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.SUSPENSION_START: {
                "prefix": "suspension_start",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.SUSPENSION_END: {
                "prefix": "suspension_end",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.OFFICER_LEFT: {
                "prefix": "resignation",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid", "left_reason"],
            },
            events.OFFICER_LEFT: {
                "prefix": "arrest",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid", "left_reason"],
            },
            events.OFFICER_LEFT: {
                "prefix": "termination",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid", "left_reason"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        pprr,
        {events.OFFICER_HIRE: {"prefix": "hire", "keep": ["uid", "agency"]}},
        ["uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    post_events = pd.read_csv(bolo.data("match/post_event_new_orleans_so.csv"))
    cprr19 = pd.read_csv(bolo.data("match/cprr_new_orleans_so_2019.csv"))
    cprr20 = pd.read_csv(bolo.data("match/cprr_new_orleans_so_2020.csv"))
    pprr = pd.read_csv(bolo.data("clean/pprr_new_orleans_so_2021.csv"))
    personnel_df = fuse_personnel(cprr20, cprr19, pprr)
    events_df = fuse_events(cprr19, cprr20, pprr)
    events_df = rearrange_event_columns(pd.concat([post_events, events_df]))
    complaint_df = rearrange_allegation_columns(pd.concat([cprr19, cprr20]))
    personnel_df.to_csv(bolo.data("fuse/per_new_orleans_so.csv"), index=False)
    events_df.to_csv(bolo.data("fuse/event_new_orleans_so.csv"), index=False)
    complaint_df.to_csv(bolo.data("fuse/com_new_orleans_so.csv"), index=False)
