import pandas as pd

import deba
from lib.columns import (
    rearrange_allegation_columns,
    rearrange_citizen_columns,
    rearrange_event_columns,
    rearrange_use_of_force,
)
from lib import events
from lib.personnel import fuse_personnel
from lib.post import load_for_agency


def fuse_events(pprr, cprr, uof25):
    builder = events.Builder()
    builder.extract_events(
        pprr,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": [
                    "uid",
                    "agency",
                    "employee_id",
                    "department_desc",
                    "salary",
                    "salary_freq",
                ],
            },
            events.OFFICER_LEFT: {
                "prefix": "termination",
                "parse_date": True,
                "keep": [
                    "uid",
                    "agency",
                    "employee_id",
                    "department_desc",
                    "salary",
                    "salary_freq",
                ],
            },
        },
        ["uid"],
    )
    builder.extract_events(
        cprr,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "parse_date": True,
                "keep": ["uid", "allegation_uid", "agency"],
            }
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        uof25,
        {
            events.UOF_INCIDENT: {
                "prefix": "occurred",
                "keep": [
                    "uid",
                    "uof_uid",
                    "agency",
                    "use_of_force_description",
                    "use_of_force_reason",
                ],
            },
        },
        ["uid", "uof_uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    pprr = pd.read_csv(deba.data("clean/pprr_ponchatoula_pd_2010_2020.csv"))
    citizen_df = pd.read_csv(deba.data("clean/uof_cit_ponchatoula_pd_2025.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    cprr = pd.read_csv(deba.data("match/cprr_ponchatoula_pd_2010_2020.csv"))
    uof25 = pd.read_csv(deba.data("match/uof_ponchatoula_pd_2025.csv"))
    post_event = pd.read_csv(deba.data("match/post_event_ponchatoula_pd_2020.csv"))
    event_df = rearrange_event_columns(
        pd.concat([fuse_events(pprr, cprr, uof25), post_event]))
    per_df = fuse_personnel(pprr, cprr, post, uof25)
    per_df.to_csv(deba.data("fuse_agency/per_ponchatoula_pd.csv"), index=False)
    com_df = rearrange_allegation_columns(cprr)
    uof25 = rearrange_use_of_force(uof25)
    citizen_df = rearrange_citizen_columns(citizen_df)
    uof25.to_csv(deba.data("fuse_agency/uof_ponchatoula_pd.csv"), index=False)
    citizen_df.to_csv(deba.data("fuse_agency/cit_ponchatoula_pd.csv"), index=False)
    com_df.to_csv(deba.data("fuse_agency/com_ponchatoula_pd.csv"), index=False)
    event_df.to_csv(deba.data("fuse_agency/event_ponchatoula_pd.csv"), index=False)
    post.to_csv(deba.data("fuse_agency/post_ponchatoula_pd.csv"), index=False)
