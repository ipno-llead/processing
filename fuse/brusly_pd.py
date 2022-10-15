import pandas as pd
import deba
from lib.columns import (
    rearrange_personnel_columns,
    rearrange_allegation_columns,
    rearrange_event_columns,
)
from lib.personnel import fuse_personnel
from lib.uid import gen_uid
from lib import events
from lib.post import load_for_agency


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
    pprr = pd.read_csv(deba.data("clean/pprr_brusly_pd_2020.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    post_event = pd.read_csv(deba.data("match/post_event_brusly_pd_2020.csv"))
    cprr = pd.read_csv(deba.data("match/cprr_brusly_pd_2020.csv"))
    award = pd.read_csv(deba.data("match/award_brusly_pd_2021.csv"))
    cprr = gen_uid(
        cprr, ["uid", "occur_year", "occur_month", "occur_day"], "allegation_uid"
    )
    events_df = fuse_events(pprr, cprr, award)
    events_df = rearrange_event_columns(pd.concat([post_event, events_df]))
    com_df = rearrange_allegation_columns(cprr)
    per_df = rearrange_personnel_columns(pprr)
    per_df = fuse_personnel(per_df, post)
    per_df.to_csv(deba.data("fuse/per_brusly_pd.csv"), index=False)
    events_df.to_csv(deba.data("fuse/event_brusly_pd.csv"), index=False)
    com_df.to_csv(deba.data("fuse/com_brusly_pd.csv"), index=False)
    post.to_csv(deba.data("fuse/post_brusly_pd.csv"), index=False)
