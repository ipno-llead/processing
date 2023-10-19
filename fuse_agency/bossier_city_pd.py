import pandas as pd
from lib.personnel import fuse_personnel
from lib.columns import (
    rearrange_allegation_columns,
    rearrange_event_columns,
    rearrange_citizen_columns,
)
import deba
from lib import events
from lib.post import load_for_agency


def fuse_events(pprr, cprr):
    builder = events.Builder()
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
    builder.extract_events(
        cprr,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": [
                    "uid",
                    "allegation_uid",
                    "agency",
                    "charges",
                    "disposition",
                    "action",
                ],
            },
            events.INVESTIGATION_START: {
                "prefix": "investigation_start",
                "keep": [
                    "uid",
                    "allegation_uid",
                    "agency",
                    "charges",
                    "disposition",
                    "action",
                ],
            },
            events.INVESTIGATION_COMPLETE: {
                "prefix": "investigation_complete",
                "keep": [
                    "uid",
                    "allegation_uid",
                    "agency",
                    "charges",
                    "disposition",
                    "action",
                ],
            },
        },
        ["uid", "allegation_uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    pprr = pd.read_csv(deba.data("clean/pprr_bossier_city_pd_2000_2019.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    post_event = pd.read_csv(deba.data("match/post_event_bossier_city_pd.csv"))
    cprr = pd.read_csv(deba.data("match/cprr_bossier_city_pd_2010_2020.csv"))
    per_df = fuse_personnel(pprr, cprr, post)
    com_df = rearrange_allegation_columns(cprr)
    events_df = fuse_events(pprr, cprr)
    events_df = rearrange_event_columns(
        pd.concat(
            [
                events_df,
                post_event,
            ]
        )
    )
    per_df.to_csv(deba.data("fuse_agency/per_bossier_city_pd.csv"), index=False)
    events_df.to_csv(deba.data("fuse_agency/event_bossier_city_pd.csv"), index=False)
    com_df.to_csv(deba.data("fuse_agency/com_bossier_city_pd.csv"), index=False)
    post.to_csv(deba.data("fuse_agency/post_bossier_city_pd.csv"), index=False)