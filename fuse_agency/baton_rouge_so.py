import pandas as pd
import deba
from lib.columns import rearrange_allegation_columns, rearrange_citizen_columns, rearrange_settlement_columns
from lib.personnel import fuse_personnel
from lib.post import load_for_agency
from lib import events


def fuse_events(cprr_15, cprr_18, cprr_21, post, settlements):
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
        settlements,
        {
            events.CLAIM_CLOSED: {
                "prefix": "claim_close",
                "parse_date": True,
                "keep": ["uid", "agency", "settlement_uid"],
            },
        },
        ["uid", "settlement_uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr_15 = pd.read_csv(deba.data("match/cprr_baton_rouge_so_2011_2015.csv"))
    cprr_18 = pd.read_csv(deba.data("match/cprr_baton_rouge_so_2018.csv"))
    cprr_20 = pd.read_csv(deba.data("match/cprr_baton_rouge_so_2016_2020.csv"))
    citizen_df15 = pd.read_csv(deba.data("clean/cprr_cit_baton_rouge_so_2011_2015.csv"))
    citizen_df18 = pd.read_csv(deba.data("clean/cprr_cit_baton_rouge_so_2018.csv"))
    citizen_df20 = pd.read_csv(deba.data("clean/cprr_cit_baton_rouge_so_2016_2020.csv"))
    settlements_df = pd.read_csv(deba.data("match/settlements_baton_rouge_so_2021_2023.csv"))
    agency = cprr_20.agency[0]
    post = load_for_agency(agency)
    personnel_df = fuse_personnel(cprr_15, cprr_18, cprr_20, post, settlements_df)
    event_df = fuse_events(cprr_15, cprr_18, cprr_20, post, settlements_df)
    complaint_df = rearrange_allegation_columns(pd.concat([cprr_15, cprr_18, cprr_20], axis=0))
    citizen_df = rearrange_citizen_columns(
        pd.concat([citizen_df15, citizen_df18, citizen_df20])
    )
    settlements_df = rearrange_settlement_columns(settlements_df)
    personnel_df.to_csv(deba.data("fuse_agency/per_baton_rouge_so.csv"), index=False)
    event_df.to_csv(deba.data("fuse_agency/event_baton_rouge_so.csv"), index=False)
    complaint_df.to_csv(deba.data("fuse_agency/com_baton_rouge_so.csv"), index=False)
    citizen_df.to_csv(deba.data("fuse_agency/cit_baton_rouge_so.csv"), index=False)
    settlements_df.to_csv(deba.data("fuse_agency/settlements_baton_rouge_so.csv"), index=False)
