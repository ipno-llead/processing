import pandas as pd
import deba
from lib import events
from lib.columns import (
    rearrange_allegation_columns,
    rearrange_use_of_force,
    rearrange_citizen_columns,
)
from lib.personnel import fuse_personnel
from lib.post import load_for_agency


def fuse_events(cprr, post, uof):
    builder = events.Builder()
    builder.extract_events(
        cprr,
        {
            events.INVESTIGATION_START: {
                "prefix": "investigation_start",
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.INVESTIGATION_COMPLETE: {
                "prefix": "investigation_complete",
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.APPEAL_DISPOSITION: {
                "prefix": "disposition",
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
                "keep": ["uid", "agency", "employment_status"],
            },
        },
        ["uid"],
    )
    builder.extract_events(
        uof,
        {
            events.UOF_INCIDENT: {
                "prefix": "occurred",
                "parse_date": True,
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
    cprr = pd.read_csv(deba.data("clean/cprr_terrebonne_so_2019_2021.csv"))
    uof = pd.read_csv(deba.data("match/uof_terrebonne_so_2021.csv"))
    citizen_df = pd.read_csv(deba.data("clean/uof_cit_terrebonne_so_2021.csv"))
    agency = cprr.agency[0]
    post = load_for_agency(agency)
    per = fuse_personnel(cprr, post, uof)
    com = rearrange_allegation_columns(cprr)
    event = fuse_events(cprr, post, uof)
    uof = rearrange_use_of_force(uof)
    citizen_df = rearrange_citizen_columns(citizen_df)
    event.to_csv(deba.data("fuse_agency/event_terrebonne_so.csv"), index=False)
    com.to_csv(deba.data("fuse_agency/com_terrebonne_so.csv"), index=False)
    per.to_csv(deba.data("fuse_agency/per_terrebonne_so.csv"), index=False)
    uof.to_csv(deba.data("fuse_agency/uof_terrebonne_so.csv"), index=False)
    citizen_df.to_csv(deba.data("fuse_agency/cit_terrebonne_so.csv"), index=False)
