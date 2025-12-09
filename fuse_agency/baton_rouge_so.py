import pandas as pd
import deba
from lib.columns import rearrange_allegation_columns, rearrange_citizen_columns, rearrange_use_of_force, rearrange_settlement_columns, rearrange_stop_and_search_columns
from lib.personnel import fuse_personnel
from lib.post import load_for_agency
from lib import events


def fuse_events(cprr_15, cprr_25, cprr_18, cprr_20, cprr_21, post, uof, uof25, settlement, sas):
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
        cprr_25,
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
        cprr_20,
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
            events.COMPLAINT_INCIDENT: {
                "prefix": "incident",
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.OFFICER_LEFT: {
                "prefix": "resign",
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
    builder.extract_events(
        uof25,
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
    builder.extract_events(
        settlement,        
        {
            events.SETTLEMENT_CHECK: {
                'prefix': "claim",
                "keep": [
                    "uid",
                    "settlement_uid",
                    "agency",
                ],
        },
        },
        ["uid", "settlement_uid"],
    )
    builder.extract_events(
        sas,        
        {
            events.STOP_AND_SEARCH: {
                'prefix': "stop",
                "keep": [
                    "uid",
                    "stop_and_search_uid",
                    "agency",
                ],
        },
        },
        ["uid", "stop_and_search_uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr_15 = pd.read_csv(deba.data("match/cprr_baton_rouge_so_2011_2015.csv"))
    cprr_18 = pd.read_csv(deba.data("match/cprr_baton_rouge_so_2018.csv"))
    cprr_20 = pd.read_csv(deba.data("match/cprr_baton_rouge_so_2016_2020.csv"))
    cprr_21 = pd.read_csv(deba.data("match/cprr_baton_rouge_so_2021_2023.csv"))
    cprr_25 = pd.read_csv(deba.data("match/cprr_baton_rouge_so_2024_2025.csv"))
    citizen_df25 = pd.read_csv(deba.data("clean/cprr_cit_baton_rouge_so_2024_2025.csv"))
    citizen_df15 = pd.read_csv(deba.data("clean/cprr_cit_baton_rouge_so_2011_2015.csv"))
    citizen_df18 = pd.read_csv(deba.data("clean/cprr_cit_baton_rouge_so_2018.csv"))
    citizen_df20 = pd.read_csv(deba.data("clean/cprr_cit_baton_rouge_so_2016_2020.csv"))
    uof = pd.read_csv(deba.data("match/uof_baton_rouge_so_2020.csv"))
    uof25 = pd.read_csv(deba.data("match/uof_baton_rouge_so_2021_2025.csv"))
    settlement = pd.read_csv(deba.data("match/settlements_baton_rouge_so_2021_2023.csv"))
    sas = pd.read_csv(deba.data("match/sas_baton_rouge_so_2023_2025.csv"))
    settlement = rearrange_settlement_columns(settlement)
    sas = rearrange_stop_and_search_columns(sas)
    agency = cprr_20.agency[0]
    post = load_for_agency(agency)
    personnel_df = fuse_personnel(cprr_15, cprr_18, cprr_20, cprr_21, cprr_25, post, uof, uof25)
    event_df = fuse_events(cprr_15, cprr_25, cprr_18, cprr_20, cprr_21, post, uof, uof25, settlement, sas)
    complaint_df = rearrange_allegation_columns(pd.concat([cprr_15, cprr_25, cprr_18, cprr_20, cprr_21], axis=0))
    citizen_df = rearrange_citizen_columns(
        pd.concat([citizen_df25, citizen_df15, citizen_df18, citizen_df20])
    )
    uof = rearrange_use_of_force(pd.concat([uof, uof25])
    )
    personnel_df.to_csv(deba.data("fuse_agency/per_baton_rouge_so.csv"), index=False)
    event_df.to_csv(deba.data("fuse_agency/event_baton_rouge_so.csv"), index=False)
    complaint_df.to_csv(deba.data("fuse_agency/com_baton_rouge_so.csv"), index=False)
    citizen_df.to_csv(deba.data("fuse_agency/cit_baton_rouge_so.csv"), index=False)
    uof.to_csv(deba.data("fuse_agency/uof_baton_rouge_so.csv"), index=False)
    settlement.to_csv(deba.data("fuse_agency/settlements_baton_rouge_so.csv"), index=False)
    sas.to_csv(deba.data("fuse_agency/sas_baton_rouge_so.csv"), index=False)
