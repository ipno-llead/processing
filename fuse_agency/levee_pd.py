from lib.columns import rearrange_allegation_columns, rearrange_citizen_columns, rearrange_use_of_force
from lib.personnel import fuse_personnel
import deba
from lib.post import load_for_agency
from lib import events
import pandas as pd

def fuse_events(cprr, post_east_jefferson,post_orleans, pprr, uof):
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
        post_east_jefferson,
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
    builder.extract_events(
        post_orleans,
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
    builder.extract_events(
        pprr, 
        { 
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": ["uid", "agency", "rank_desc", "badge_no"],
            },
            events.OFFICER_LEFT: {
                "prefix": "termination",
                "keep": ["uid", "agency", "rank_desc", "badge_no"],
            },
        },
        ["uid", "agency", "rank_desc", "badge_no"],
    )
    builder.extract_events(
        uof,
        {
            events.UOF_INCIDENT: {
                "prefix": "incident",
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
    cprr = pd.read_csv(deba.data("match/cprr_levee_pd.csv"))
    pprr = pd.read_csv(deba.data("match/pprr_levee_pd_1980_2025.csv"))
    citizen_df = pd.read_csv(deba.data("clean/cprr_cit_levee_pd.csv"))
    uof = pd.read_csv(deba.data("clean/uof_levee_pd_2020_2025.csv"))

    post_east_jefferson = load_for_agency("east-jefferson-levee-pd")
    post_orleans = load_for_agency("orleans-levee-pd")
    cprr = cprr[~((cprr.uid.fillna("") == ""))]

    event_df = fuse_events(cprr, post_orleans, post_east_jefferson, pprr, uof)
    per_df = fuse_personnel(post_orleans, post_east_jefferson, cprr, pprr, uof)
    complaint_df = rearrange_allegation_columns(cprr)
    citizen_df = rearrange_citizen_columns(citizen_df)
    uof = rearrange_use_of_force(uof)

    event_df.to_csv(deba.data("fuse_agency/event_levee_pd.csv"), index=False)
    complaint_df.to_csv(deba.data("fuse_agency/com_levee_pd.csv"), index=False)
    per_df.to_csv(deba.data("fuse_agency/per_levee_pd.csv"), index=False)
    citizen_df.to_csv(deba.data("fuse_agency/cit_levee_pd.csv"), index=False)
    uof.to_csv(deba.data("fuse_agency/uof_levee_pd.csv"), index=False)
