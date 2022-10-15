import pandas as pd
import deba

from lib import events
from lib.personnel import fuse_personnel
from lib.columns import rearrange_allegation_columns, rearrange_event_columns
from lib.post import load_for_agency


def fuse_events(cprr_20, cprr_14, cprr_08, pprr):
    builder = events.Builder()
    builder.extract_events(
        cprr_20,
        {
            events.INVESTIGATION_START: {
                "prefix": "investigation_start",
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.COMPLAINT_INCIDENT: {
                "prefix": "incident",
                "keep": ["uid", "agency", "allegation_uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr_14,
        {
            events.INVESTIGATION_START: {
                "prefix": "investigation_start",
                "keep": ["uid", "agency", "allegation_uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr_08,
        {
            events.COMPLAINT_INCIDENT: {
                "prefix": "incident",
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.INITIAL_ACTION: {
                "prefix": "initial_action",
                "keep": ["uid", "agency", "allegation_uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        pprr,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": ["uid", "agency", "rank_desc", "salary", "salary_freq"],
            },
        },
        ["uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr_20 = pd.read_csv(deba.data("match/cprr_hammond_pd_2015_2020.csv"))
    cprr_14 = pd.read_csv(deba.data("match/cprr_hammond_pd_2009_2014.csv"))
    cprr_08 = pd.read_csv(deba.data("clean/cprr_hammond_pd_2004_2008.csv"))
    post_event = pd.read_csv(deba.data("match/post_event_hammond_pd_2020_11_06.csv"))
    pprr = pd.read_csv(deba.data("clean/pprr_hammond_pd_2021.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    personnel_df = fuse_personnel(cprr_20, cprr_14, cprr_08, pprr, post)
    complaints_df = rearrange_allegation_columns(pd.concat([cprr_20, cprr_14, cprr_08]))
    event_df = fuse_events(cprr_20, cprr_14, cprr_08, pprr)
    event_df = rearrange_event_columns(pd.concat([event_df, post_event]))
    event_df.to_csv(deba.data("fuse/event_hammond_pd.csv"), index=False)
    personnel_df.to_csv(deba.data("fuse/per_hammond_pd.csv"), index=False)
    complaints_df.to_csv(deba.data("fuse/com_hammond_pd.csv"), index=False)
    post.to_csv(deba.data("fuse/post_hammond_pd.csv"), index=False)
