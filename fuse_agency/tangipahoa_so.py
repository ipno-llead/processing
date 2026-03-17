import deba
from lib.columns import (
    rearrange_allegation_columns,
    rearrange_citizen_columns,
    rearrange_use_of_force,
)
from lib import events
from lib.personnel import fuse_personnel
from lib.post import load_for_agency
import pandas as pd


def fuse_events(cprr21, post, uof_24, uof_25):
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
    builder.extract_events(
        uof_24,
        {
            events.UOF_INCIDENT: {
                "prefix": "occurred",
                "parse_date": "%Y-%m-%d",
                "keep": [
                    "uid",
                    "uof_uid",
                    "agency",
                ],
            },
        },
        ["uid", "uof_uid"],
    )
    builder.extract_events(
        uof_25,
        {
            events.UOF_INCIDENT: {
                "prefix": "occurred",
                "parse_date": "%Y-%m-%d",
                "keep": [
                    "uid",
                    "uof_uid",
                    "agency",
                ],
            },
        },
        ["uid", "uof_uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr21 = pd.read_csv(deba.data("match/cprr_tangipahoa_so_2015_2021.csv"))
    cprr13 = pd.read_csv(deba.data("match/tangipahoa_so_cprr_2013.csv"))
    uof_24 = pd.read_csv(deba.data("match/uof_tangipahoa_so_2024.csv"))
    uof_25 = pd.read_csv(deba.data("match/uof_tangipahoa_so_2025.csv"))
    cprr_citizen_df = pd.read_csv(deba.data("clean/cprr_cit_tangipahoa_so_2015_2021.csv"))
    uof_citizen_24 = pd.read_csv(deba.data("clean/uof_cit_tangipahoa_so_2024.csv"))
    uof_citizen_25 = pd.read_csv(deba.data("clean/uof_cit_tangipahoa_so_2025.csv"))
    agency = cprr21.agency[0]
    post = load_for_agency(agency)
    per = fuse_personnel(cprr21, cprr13, post, uof_24, uof_25)
    complaints = rearrange_allegation_columns(pd.concat([cprr21, cprr13], axis=0))
    event = fuse_events(cprr21, post, uof_24, uof_25)
    combined_uof = pd.concat([uof_24, uof_25], ignore_index=True)
    uof_df = rearrange_use_of_force(combined_uof)
    combined_cit = pd.concat(
        [cprr_citizen_df, uof_citizen_24, uof_citizen_25], ignore_index=True
    )
    citizen_df = rearrange_citizen_columns(combined_cit)
    event.to_csv(deba.data("fuse_agency/event_tangipahoa_so.csv"), index=False)
    complaints.to_csv(deba.data("fuse_agency/com_tangipahoa_so.csv"), index=False)
    per.to_csv(deba.data("fuse_agency/per_tangipahoa_so.csv"), index=False)
    uof_df.to_csv(deba.data("fuse_agency/uof_tangipahoa_so.csv"), index=False)
    citizen_df.to_csv(deba.data("fuse_agency/cit_tangipahoa_so.csv"), index=False)
