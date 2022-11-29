from lib.columns import rearrange_allegation_columns, rearrange_citizen_columns
from lib.personnel import fuse_personnel
import deba
from lib.post import load_for_agency
from lib import events
import pandas as pd


def fuse_events(cprr, post):
    builder = events.Builder()
    builder.extract_events(
        cprr,
        {
            events.COMPLAINT_INCIDENT: {
                "prefix": "occur",
                "parse_date": True,
                "keep": ["agency", "uid", "allegation_uid"],
            },
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "parse_date": True,
                "keep": ["agency", "uid", "allegation_uid"],
            },
            events.INVESTIGATION_COMPLETE: {
                "prefix": "investigation_complete",
                "parse_date": True,
                "keep": ["agency", "uid", "allegation_uid"],
            },
            events.INVESTIGATION_START: {
                "prefix": "investigation_start",
                "parse_date": True,
                "keep": ["agency", "uid", "allegation_uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        post,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": ["agency", "uid", "employment_status"],
            },
            events.OFFICER_LEVEL_1_CERT: {
                "prefix": "level_1_cert",
                "parse_date": "%Y-%m-%d",
                "keep": ["agency", "uid"],
            },
            events.OFFICER_PC_12_QUALIFICATION: {
                "prefix": "last_pc_12_qualification",
                "parse_date": "%Y-%m-%d",
                "keep": ["agency", "uid"],
            },
        },
        ["uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr = pd.read_csv(deba.data("clean/cprr_levee_pd.csv"))
    citizen_df = pd.read_csv(deba.data("clean/cprr_cit_levee_pd.csv"))
    agency = cprr.agency[0]
    cprr = cprr[~((cprr.uid.fillna("") == ""))]
    post = load_for_agency(agency)
    event_df = fuse_events(cprr, post)
    event_df.to_csv(deba.data("fuse/event_levee_pd.csv"), index=False)
    complaint_df = rearrange_allegation_columns(cprr)
    citizen_df = rearrange_citizen_columns(citizen_df)
    complaint_df.to_csv(deba.data("fuse/com_levee_pd.csv"), index=False)
    per_df = fuse_personnel(post, cprr)
    per_df.to_csv(deba.data("fuse/per_levee_pd.csv"), index=False)
    citizen_df.to_csv(deba.data("fuse/cit_levee_pd.csv"), index=False)
