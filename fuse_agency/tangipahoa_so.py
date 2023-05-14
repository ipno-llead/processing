import deba
from lib.columns import rearrange_allegation_columns, rearrange_citizen_columns
from lib import events
from lib.personnel import fuse_personnel
from lib.post import load_for_agency
import pandas as pd


def fuse_events(cprr21, post):
    builder = events.Builder()
    builder.extract_events(
        cprr21,
        {
            events.INVESTIGATION_COMPLETE: {
                "prefix": "investigation_complete",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid"],
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
                "keep": [
                    "uid",
                    "agency",
                    "employment_status",
                ],
            },
        },
        ["uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr21 = pd.read_csv(deba.data("match/cprr_tangipahoa_so_2015_2021.csv"))
    cprr13 = pd.read_csv(deba.data("match/tangipahoa_so_cprr_2013.csv"))
    citizen_df = pd.read_csv(deba.data("clean/cprr_cit_tangipahoa_so_2015_2021.csv"))
    agency = cprr21.agency[0]
    post = load_for_agency(agency)
    per = fuse_personnel(cprr21, cprr13, post)
    complaints = rearrange_allegation_columns(pd.concat([cprr21, cprr13], axis=0))
    event = fuse_events(cprr21, post)
    citizen_df = rearrange_citizen_columns(citizen_df)
    event.to_csv(deba.data("fuse_agency/event_tangipahoa_so.csv"), index=False)
    complaints.to_csv(deba.data("fuse_agency/com_tangipahoa_so.csv"), index=False)
    per.to_csv(deba.data("fuse_agency/per_tangipahoa_so.csv"), index=False)
    citizen_df.to_csv(deba.data("fuse_agency/cit_tangipahoa_so.csv"), index=False)
