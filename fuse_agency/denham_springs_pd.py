import pandas as pd
import deba
from lib import events
from lib.columns import (
    rearrange_allegation_columns,
    rearrange_event_columns,
    rearrange_use_of_force,
    rearrange_citizen_columns,
)
from lib.personnel import fuse_personnel
from lib.post import load_for_agency


def fuse_events(cprr, uof, post):
    builder = events.Builder()
    builder.extract_events(
        cprr,
        {
            events.COMPLAINT_INCIDENT: {
                "prefix": "incident",
                "keep": ["uid", "agency", "allegation_uid"],
            }
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        uof,
        {
            events.UOF_INCIDENT: {
                "prefix": "occurred",
                "keep": ["uid", "uof_uid", "agency"],
            },
        },
        ["uid", "uof_uid"],
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
    cprr = pd.read_csv(deba.data("match/cprr_denham_springs_pd_2016_2021.csv"))
    uof = pd.read_csv(deba.data("match/uof_denham_springs_pd_2025_2026.csv"))
    citizen_df = pd.read_csv(deba.data("clean/uof_cit_denham_springs_pd_2025_2026.csv"))
    agency = cprr.agency[0]
    post = load_for_agency(agency)
    per = fuse_personnel(cprr, uof, post)
    com = rearrange_allegation_columns(cprr)
    event = rearrange_event_columns(fuse_events(cprr, uof, post))
    uof_df = rearrange_use_of_force(uof)
    citizen_df = rearrange_citizen_columns(citizen_df)
    event.to_csv(deba.data("fuse_agency/event_denham_springs_pd.csv"), index=False)
    com.to_csv(deba.data("fuse_agency/com_denham_springs_pd.csv"), index=False)
    per.to_csv(deba.data("fuse_agency/per_denham_springs_pd.csv"), index=False)
    uof_df.to_csv(deba.data("fuse_agency/uof_denham_springs_pd.csv"), index=False)
    citizen_df.to_csv(deba.data("fuse_agency/cit_denham_springs_pd.csv"), index=False)
