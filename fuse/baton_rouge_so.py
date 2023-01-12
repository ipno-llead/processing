import pandas as pd
import deba
from lib.columns import rearrange_allegation_columns, rearrange_citizen_columns, rearrange_use_of_force
from lib.personnel import fuse_personnel
from lib.post import load_for_agency
from lib import events


def fuse_events(cprr_15, cprr_18, cprr_21, post, uof20):
    builder = events.Builder()
    builder.extract_events(
        cprr_15,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "allegation_uid", "badge_no", "rank_desc"],
            },
            events.COMPLAINT_INCIDENT: {
                "prefix": "occur",
                "keep": ["uid", "agency", "allegation_uid", "badge_no", "rank_desc"],
            },
            events.OFFICER_RANK: {
                "prefix": "rank",
                "keep": ["uid", "agency", "badge_no", "rank_desc"],
                "id_cols": ["uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr_18,
        {
            events.OFFICER_RANK: {
                "prefix": "rank",
                "keep": ["uid", "agency", "badge_no", "rank_desc"],
                "id_cols": ["uid"],
            },
            events.COMPLAINT_INCIDENT: {
                "prefix": "occur",
                "keep": ["uid", "agency", "allegation_uid", "badge_no", "rank_desc"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr_21,
        {
            events.OFFICER_RANK: {
                "prefix": "rank",
                "keep": ["uid", "agency", "badge_no", "rank_desc"],
                "id_cols": ["uid"],
            },
            events.COMPLAINT_INCIDENT: {
                "prefix": "occur",
                "keep": ["uid", "agency", "allegation_uid", "badge_no", "rank_desc"],
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
                "keep": ["uid", "agency"],
            },
            events.OFFICER_PC_12_QUALIFICATION: {
                "prefix": "last_pc_12_qualification",
                "parse_date": "%Y-%m-%d",
                "keep": ["uid", "agency"],
            },
            events.OFFICER_HIRE: {"prefix": "hire", "keep": ["uid", "agency"]},
        },
        ["uid"],
    )
    builder.extract_events(
        uof20,
        {
            events.REPORT_DATE: {
                "prefix": "report",
                "parse_date": True,
                "keep": ["uid", "uof_uid", "use_of_force_description", "use_of_force_effective", "agency", ],
            },
        },
        ["uid", "uof_uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr_15 = pd.read_csv(deba.data("match/cprr_baton_rouge_so_2011_2015.csv"))
    cprr_18 = pd.read_csv(deba.data("match/cprr_baton_rouge_so_2018.csv"))
    cprr_20 = pd.read_csv(deba.data("match/cprr_baton_rouge_so_2016_2020.csv"))
    citizen_df15 = pd.read_csv(deba.data("clean/cprr_cit_baton_rouge_so_2011_2015.csv"))
    citizen_df18 = pd.read_csv(deba.data("clean/cprr_cit_baton_rouge_so_2018.csv"))
    citizen_df20 = pd.read_csv(deba.data("clean/cprr_cit_baton_rouge_so_2016_2020.csv"))
    uof20 = pd.read_csv(deba.data("match/uof_baton_rouge_so_2020.csv"))
    agency = cprr_20.agency[0]
    post = load_for_agency(agency)
    personnel_df = fuse_personnel(cprr_15, cprr_18, cprr_20, post)
    event_df = fuse_events(cprr_15, cprr_18, cprr_20, post, uof20)
    complaint_df = rearrange_allegation_columns(pd.concat([cprr_15, cprr_18, cprr_20], axis=0))
    citizen_df = rearrange_citizen_columns(
        pd.concat([citizen_df15, citizen_df18, citizen_df20])
    )
    uof20 = rearrange_use_of_force(uof20)
    personnel_df.to_csv(deba.data("fuse/per_baton_rouge_so.csv"), index=False)
    event_df.to_csv(deba.data("fuse/event_baton_rouge_so.csv"), index=False)
    complaint_df.to_csv(deba.data("fuse/com_baton_rouge_so.csv"), index=False)
    citizen_df.to_csv(deba.data("fuse/cit_baton_rouge_so.csv"), index=False)
    uof20.to_csv(deba.data("fuse/uof_baton_rouge_so.csv"), index=False)
