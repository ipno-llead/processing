import deba
from lib.columns import rearrange_allegation_columns, rearrange_award_columns, rearrange_use_of_force, rearrange_citizen_columns
from lib import events
from lib.personnel import fuse_personnel
from lib.post import load_for_agency
import pandas as pd


def fuse_events(award17, cprr20, cprr14, cprr08, post, uof):
    builder = events.Builder()
    builder.extract_events(
        award17,
        {
            events.AWARD_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "award_uid", "agency"],
            },
        },
        ["uid", "award_uid"],
    )
    builder.extract_events(
        cprr20,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "allegation_uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr14,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr08,
        {
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
                "keep": ["uid", "agency", "employment_status"],
            },
        },
        ["uid"],
    )
    builder.extract_events(
        uof,
        {
            events.UOF_INCIDENT: {
                "prefix": "occur",
                "keep": ["uid", "agency", "use_of_force_description", "disposition", "use_of_force_reason"],
            },
        },
        ["uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr20 = pd.read_csv(deba.data("match/cprr_lafayette_so_2015_2020.csv"))
    cprr14 = pd.read_csv(deba.data("match/cprr_lafayette_so_2009_2014.csv"))
    cprr08 = pd.read_csv(deba.data("match/cprr_lafayette_so_2006_2008.csv"))
    award17 = pd.read_csv(deba.data("match/award_lafayette_so_2017.csv"))
    uof = pd.read_csv(deba.data("match/uof_lafayette_so_2015_2019.csv"))
    uof_citizens = pd.read_csv(deba.data("clean/uof_citizens_lafayette_so_2015_2019.csv"))
    agency = cprr08.agency[0]
    post = load_for_agency(agency)
    complaints = rearrange_allegation_columns(pd.concat([cprr20, cprr14, cprr08], axis=0))
    event = fuse_events(award17, cprr20, cprr14, cprr08, post, uof)
    personnel_df = fuse_personnel(cprr20, cprr14, cprr08, post, award17, uof)
    award_df = rearrange_award_columns(award17)
    uof_citizens = rearrange_citizen_columns(uof_citizens)
    uof = rearrange_use_of_force(uof)
    award_df.to_csv(deba.data("fuse_agency/award_lafayette_so.csv"), index=False)
    personnel_df.to_csv(deba.data("fuse_agency/per_lafayette_so.csv"), index=False)
    event.to_csv(deba.data("fuse_agency/event_lafayette_so.csv"), index=False)
    complaints.to_csv(deba.data("fuse_agency/com_lafayette_so.csv"), index=False)
    uof.to_csv(deba.data("fuse_agency/uof_lafayette_so.csv"), index=False)
    uof_citizens.to_csv(deba.data("fuse_agency/cit_lafayette_so.csv"), index=False)
