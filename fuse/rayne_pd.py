import pandas as pd
import deba

from lib import events
from lib.columns import rearrange_allegation_columns, rearrange_event_columns
from lib.personnel import fuse_personnel
from lib.post import load_for_agency

def fuse_events(cprr14, cprr20, pprr, cprr13):
    builder = events.Builder()
    builder.extract_events(
        cprr14,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.INVESTIGATION_COMPLETE: {
                "prefix": "investigation_complete",
                "keep": ["uid", "agency", "allegation_uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr20,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.INVESTIGATION_COMPLETE: {
                "prefix": "investigation_complete",
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
                "keep": ["uid", "agency", "rank_desc"],
            },
            events.OFFICER_LEFT: {
                "prefix": "left",
                "keep": ["uid", "agency", "rank_desc", "left_reason"],
            },
        },
        ["uid"],
    )
    builder.extract_events(
        cprr13,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.INVESTIGATION_COMPLETE: {
                "prefix": "investigation_complete",
                "keep": ["uid", "agency", "allegation_uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr20 = pd.read_csv(deba.data("match/cprr_rayne_pd_2019_2020.csv"))
    cprr14 = pd.read_csv(deba.data("match/cprr_rayne_pd_2014_2018.csv"))
    cprr13 = pd.read_csv(deba.data("match/cprr_rayne_pd_2009_2013.csv"))
    pprr = pd.read_csv(deba.data("clean/pprr_rayne_pd_2010_2020.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    post_event = pd.read_csv(deba.data("match/post_event_rayne_pd_2020_11_06.csv"))
    per = fuse_personnel(cprr20, pprr, cprr14)
    com = rearrange_allegation_columns(pd.concat([cprr20, cprr14, cprr13], axis=0))
    events_df = rearrange_event_columns(
        pd.concat([post_event, fuse_events(cprr14, cprr20, pprr, cprr13)])
    )
    per.to_csv(deba.data("fuse/per_rayne_pd.csv"), index=False)
    com.to_csv(deba.data("fuse/com_rayne_pd.csv"), index=False)
    events_df.to_csv(deba.data("fuse/event_rayne_pd.csv"), index=False)
