import pandas as pd
from lib.path import data_file_path
from lib.columns import (
    rearrange_personnel_columns,
    rearrange_allegation_columns,
    rearrange_event_columns,
)
from lib.uid import gen_uid
from lib import events

import sys

sys.path.append("../")


def fuse_events(pprr, cprr, award):
    builder = events.Builder()
    builder.extract_events(
        pprr,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": ["uid", "agency", "rank_desc", "salary", "salary_freq"],
            },
            events.OFFICER_LEFT: {
                "prefix": "termination",
                "parse_date": True,
                "keep": ["uid", "agency", "rank_desc", "salary", "salary_freq"],
            },
        },
        ["uid"],
    )
    builder.extract_events(
        cprr,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.COMPLAINT_INCIDENT: {
                "prefix": "occur",
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.SUSPENSION_START: {
                "prefix": "suspension_start",
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.SUSPENSION_END: {
                "prefix": "suspension_end",
                "keep": ["uid", "agency", "allegation_uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        award,
        {
            events.AWARD_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "award", "award_comments"],
            }
        },
        ["uid", "award"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    pprr = pd.read_csv(data_file_path("clean/pprr_brusly_pd_2020.csv"))
    post_event = pd.read_csv(data_file_path("match/post_event_brusly_pd_2020.csv"))
    cprr = pd.read_csv(data_file_path("match/cprr_brusly_pd_2020.csv"))
    award = pd.read_csv(data_file_path("match/award_brusly_pd_2021.csv"))
    cprr = gen_uid(
        cprr, ["uid", "occur_year", "occur_month", "occur_day"], "allegation_uid"
    )
    events_df = fuse_events(pprr, cprr, award)
    events_df = rearrange_event_columns(pd.concat([post_event, events_df]))
    com_df = rearrange_allegation_columns(cprr)
    rearrange_personnel_columns(pprr).to_csv(
        data_file_path("fuse/per_brusly_pd.csv"), index=False
    )
    events_df.to_csv(data_file_path("fuse/event_brusly_pd.csv"), index=False)
    com_df.to_csv(data_file_path("fuse/com_brusly_pd.csv"), index=False)
