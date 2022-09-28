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


def fuse_events(pprr08, pprr20, cprr):
    builder = events.Builder()
    builder.extract_events(
        pprr08,
        {
            events.OFFICER_HIRE: {"prefix": "hire", "keep": ["uid", "agency"]},
            events.OFFICER_LEFT: {"prefix": "resign", "keep": ["uid", "agency"]},
            events.OFFICER_PAY_EFFECTIVE: {
                "prefix": "pay_effective",
                "keep": ["uid", "agency", "salary", "salary_freq"],
            },
            events.OFFICER_RANK: {
                "prefix": "rank",
                "keep": ["uid", "agency", "rank_desc"],
            },
        },
        ["uid"],
    )
    builder.extract_events(
        pprr20,
        {
            events.OFFICER_HIRE: {"prefix": "hire", "keep": ["uid", "agency"]},
            events.OFFICER_LEFT: {"prefix": "resign", "keep": ["uid", "agency"]},
            events.OFFICER_PAY_EFFECTIVE: {
                "prefix": "pay_effective",
                "keep": ["uid", "agency", "salary", "salary_freq"],
            },
            events.OFFICER_RANK: {
                "prefix": "rank",
                "keep": ["uid", "agency", "rank_desc"],
            },
        },
        ["uid"],
    )
    builder.extract_events(
        cprr,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": ["uid", "agency"],
                "id_cols": ["uid"],
            },
            events.COMPLAINT_INCIDENT: {
                "prefix": "occur",
                "keep": ["uid", "agency", "allegation_uid"],
            },
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
    return events.discard_events_occur_more_than_once_every_30_days(
        builder.to_frame(), events.OFFICER_LEFT, ["uid"]
    )


if __name__ == "__main__":
    pprr20 = pd.read_csv(deba.data("clean/pprr_new_orleans_harbor_pd_2020.csv"))
    pprr08 = pd.read_csv(deba.data("clean/pprr_new_orleans_harbor_pd_1991_2008.csv"))
    agency = pprr08.agency[0]
    post = load_for_agency(agency)
    cprr = pd.read_csv(deba.data("match/cprr_new_orleans_harbor_pd_2020.csv"))
    post_event = pd.read_csv(
        deba.data("match/post_event_new_orleans_harbor_pd_2020.csv")
    )
    personnel_df = rearrange_personnel_columns(pd.concat([pprr08, pprr20]))
    personnel_df = fuse_personnel(personnel_df, post)
    complaint_df = rearrange_allegation_columns(cprr)
    event_df = fuse_events(pprr08, pprr20, cprr)
    event_df = rearrange_event_columns(pd.concat([post_event, event_df]))
    personnel_df.to_csv(deba.data("fuse/per_new_orleans_harbor_pd.csv"), index=False)
    event_df.to_csv(deba.data("fuse/event_new_orleans_harbor_pd.csv"), index=False)
    complaint_df.to_csv(deba.data("fuse/com_new_orleans_harbor_pd.csv"), index=False)
