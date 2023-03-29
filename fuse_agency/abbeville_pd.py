import pandas as pd
import deba
from lib import events
from lib.columns import rearrange_allegation_columns
from lib.personnel import fuse_personnel
from lib.post import load_for_agency


def fuse_events(cprr21, cprr18, post):
    builder = events.Builder()
    builder.extract_events(
        cprr21,
        {
            events.INVESTIGATION_COMPLETE: {
                "prefix": "investigation_complete",
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "allegation_uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr18,
        {
            events.INVESTIGATION_COMPLETE: {
                "prefix": "investigation_complete",
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "allegation_uiid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        post,
        {
            events.OFFICER_LEVEL_1_CERT: {
                "prefix": "level_1_cert",
                "parse_date": "%Y-%m-%d",
                "keep": ["uid", "agency", "employment_status"],
            },
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": ["uid", "agency", "employment_status"],
            },
            events.OFFICER_PC_12_QUALIFICATION: {
                "prefix": "last_pc_12_qualification",
                "parse_date": "%Y-%m-%d",
                "keep": ["uid", "agency", "employment_status"],
            },
        },
        ["uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr21 = pd.read_csv(deba.data("match/cprr_abbeville_pd_2019_2021.csv"))
    cprr18 = pd.read_csv(deba.data("match/cprr_abbeville_pd_2015_2018.csv"))
    agency = cprr21.agency[0]
    post = load_for_agency(agency)
    per = fuse_personnel(cprr21, cprr18, post)
    com = rearrange_allegation_columns(pd.concat([cprr21, cprr18], axis=0))
    event = fuse_events(cprr21, cprr18, post)
    event.to_csv(deba.data("fuse_agency/event_abbeville_pd.csv"), index=False)
    com.to_csv(deba.data("fuse_agency/com_abbeville_pd.csv"), index=False)
    per.to_csv(deba.data("fuse_agency/per_abbeville_pd.csv"), index=False)
