import pandas as pd
import deba
from lib import events
from lib.columns import rearrange_allegation_columns, rearrange_citizen_columns
from lib.personnel import fuse_personnel
from lib.post import load_for_agency


def fuse_events(cprr, post, cprr24):
    builder = events.Builder()
    builder.extract_events(
        cprr,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
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
        cprr24,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "allegation_uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr = pd.read_csv(deba.data("clean/cprr_washington_so_2010_2022.csv"))
    cprr24 = pd.read_csv(deba.data("clean/cprr_washington_so_2023_2025.csv"))
    citizen_df = pd.read_csv(deba.data("clean/cprr_cit_washington_so_2010_2022.csv"))
    citizen_df_24 = pd.read_csv(deba.data("clean/cprr_cit_washington_so_2023_2025.csv"))
    agency = cprr.agency[0]
    post = load_for_agency(agency)
    per = fuse_personnel(cprr, post, cprr24)
    combined_cprr = pd.concat([cprr, cprr24])
    com = rearrange_allegation_columns(combined_cprr)
    event = fuse_events(cprr, post, cprr24)
    combined_citizen_df = pd.concat([citizen_df, citizen_df_24])
    citizen_df = rearrange_citizen_columns(combined_citizen_df)
    event.to_csv(deba.data("fuse_agency/event_washington_so.csv"), index=False)
    com.to_csv(deba.data("fuse_agency/com_washington_so.csv"), index=False)
    per.to_csv(deba.data("fuse_agency/per_washington_so.csv"), index=False)
    citizen_df.to_csv(deba.data("fuse_agency/cit_washington_so.csv"), index=False)
