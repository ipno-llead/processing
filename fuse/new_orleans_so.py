import pandas as pd

import deba
from lib.columns import rearrange_allegation_columns, rearrange_event_columns
from lib.personnel import fuse_personnel
from lib import events
from lib.post import load_for_agency


def fuse_events(cprr19, cprr20, cprr21, pprr):
    builder = events.Builder()
    builder.extract_events(
        cprr19,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "allegation_uid", "rank_desc", "employee_id"],
            },
            events.INVESTIGATION_START: {
                "prefix": "investigation_start",
                "keep": ["uid", "agency", "allegation_uid", "rank_desc", "employee_id"],
            },
            events.INVESTIGATION_COMPLETE: {
                "prefix": "investigation_complete",
                "keep": ["uid", "agency", "allegation_uid", "rank_desc", "employee_id"],
            },
            events.SUSPENSION_START: {
                "prefix": "suspension_start",
                "keep": ["uid", "agency", "allegation_uid", "rank_desc", "employee_id"],
            },
            events.SUSPENSION_END: {
                "prefix": "suspension_end",
                "keep": ["uid", "agency", "allegation_uid", "rank_desc", "employee_id"],
            },
            events.OFFICER_LEFT: {
                "prefix": "resignation",
                "keep": [
                    "uid",
                    "agency",
                    "allegation_uid",
                    "left_reason",
                    "rank_desc",
                    "employee_id",
                ],
            },
            events.OFFICER_LEFT: {
                "prefix": "arrest",
                "keep": [
                    "uid",
                    "agency",
                    "allegation_uid",
                    "left_reason",
                    "rank_desc",
                    "employee_id",
                ],
            },
            events.OFFICER_LEFT: {
                "prefix": "termination",
                "keep": [
                    "uid",
                    "agency",
                    "allegation_uid",
                    "left_reason",
                    "rank_desc",
                    "employee_id",
                ],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr20,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "allegation_uid", "rank_desc", "employee_id"],
            },
            events.INVESTIGATION_START: {
                "prefix": "investigation_start",
                "keep": ["uid", "agency", "allegation_uid", "rank_desc", "employee_id"],
            },
            events.INVESTIGATION_COMPLETE: {
                "prefix": "investigation_complete",
                "keep": ["uid", "agency", "allegation_uid", "rank_desc", "employee_id"],
            },
            events.SUSPENSION_START: {
                "prefix": "suspension_start",
                "keep": ["uid", "agency", "allegation_uid", "rank_desc", "employee_id"],
            },
            events.SUSPENSION_END: {
                "prefix": "suspension_end",
                "keep": ["uid", "agency", "allegation_uid", "rank_desc", "employee_id"],
            },
            events.OFFICER_LEFT: {
                "prefix": "resignation",
                "keep": [
                    "uid",
                    "agency",
                    "allegation_uid",
                    "left_reason",
                    "rank_desc",
                    "employee_id",
                ],
            },
            events.OFFICER_LEFT: {
                "prefix": "arrest",
                "keep": [
                    "uid",
                    "agency",
                    "allegation_uid",
                    "left_reason",
                    "rank_desc",
                    "employee_id",
                ],
            },
            events.OFFICER_LEFT: {
                "prefix": "termination",
                "keep": [
                    "uid",
                    "agency",
                    "allegation_uid",
                    "left_reason",
                    "rank_desc",
                    "employee_id",
                ],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr21,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid", "rank_desc", "employee_id"],
            },
            events.INVESTIGATION_START: {
                "prefix": "investigation_start",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid", "rank_desc", "employee_id"],
            },
            events.INVESTIGATION_COMPLETE: {
                "prefix": "investigation_complete",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid", "rank_desc", "employee_id"],
            },
            events.SUSPENSION_START: {
                "prefix": "suspension_start",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid", "rank_desc", "employee_id"],
            },
            events.SUSPENSION_END: {
                "prefix": "suspension_end",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid", "rank_desc", "employee_id"],
            },
            events.SUSPENSION_END: {
                "prefix": "return_from_suspension",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid", "rank_desc", "employee_id"],
            },
            events.OFFICER_LEFT: {
                "prefix": "resignation",
                "parse_date": True,
                "keep": [
                    "uid",
                    "agency",
                    "allegation_uid",
                    "left_reason",
                    "rank_desc",
                    "employee_id",
                ],
            },
            events.OFFICER_LEFT: {
                "prefix": "termination",
                "parse_date": True,
                "keep": [
                    "uid",
                    "agency",
                    "allegation_uid",
                    "left_reason",
                    "rank_desc",
                    "employee_id",
                ],
            },
            events.BOARD_HEARING: {
                "prefix": "board_hearing",
                "parse_date": True,
                "keep": [
                    "uid",
                    "agency",
                    "allegation_uid",
                    "left_reason",
                    "rank_desc",
                    "employee_id",
                ],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        pprr,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": ["uid", "agency", "rank_desc"],
            }
        },
        ["uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    post_events = pd.read_csv(deba.data("match/post_event_new_orleans_so.csv"))
    cprr19 = pd.read_csv(deba.data("match/cprr_new_orleans_so_2019.csv"))
    cprr20 = pd.read_csv(deba.data("match/cprr_new_orleans_so_2020.csv"))
    cprr21 = pd.read_csv(deba.data("match/cprr_new_orleans_so_2021.csv"))
    pprr = pd.read_csv(deba.data("clean/pprr_new_orleans_so_2021.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    personnel_df = fuse_personnel(cprr20, cprr19, cprr21, pprr, post)
    events_df = fuse_events(cprr19, cprr20, cprr21, pprr)
    events_df = rearrange_event_columns(pd.concat([post_events, events_df]))
    complaint_df = rearrange_allegation_columns(pd.concat([cprr19, cprr20, cprr21]))
    personnel_df.to_csv(deba.data("fuse/per_new_orleans_so.csv"), index=False)
    events_df.to_csv(deba.data("fuse/event_new_orleans_so.csv"), index=False)
    complaint_df.to_csv(deba.data("fuse/com_new_orleans_so.csv"), index=False)
    post.to_csv(deba.data("fuse/post_new_orleans_so.csv"), index=False)
