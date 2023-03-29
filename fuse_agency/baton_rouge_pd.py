import pandas as pd
import deba
from lib.personnel import fuse_personnel
from lib.columns import (
    rearrange_event_columns,
    rearrange_allegation_columns,
    rearrange_appeal_hearing_columns,
    rearrange_settlement_columns,
)
from lib import events
from lib.post import load_for_agency


def fuse_events(csd_pprr_17, csd_pprr_19, cprr_18, cprr_21, cprr_09, settlements):
    builder = events.Builder()

    builder.extract_events(
        csd_pprr_17,
        {
            events.OFFICER_HIRE: {"prefix": "hire", "keep": ["uid", "agency"]},
            events.OFFICER_LEFT: {"prefix": "resign", "keep": ["uid", "agency"]},
            events.OFFICER_PAY_EFFECTIVE: {
                "prefix": "pay_effective",
                "keep": ["uid", "agency", "salary", "salary_freq"],
            },
            events.OFFICER_RANK: {
                "prefix": "rank",
                "keep": ["uid", "agency", "rank_code", "rank_desc"],
            },
            events.OFFICER_DEPT: {
                "prefix": "dept",
                "keep": ["uid", "agency", "department_code", "department_desc"],
            },
        },
        ["uid"],
    )
    builder.extract_events(
        csd_pprr_19,
        {
            events.OFFICER_HIRE: {"prefix": "hire", "keep": ["uid", "agency"]},
            events.OFFICER_LEFT: {"prefix": "resign", "keep": ["uid", "agency"]},
            events.OFFICER_PAY_EFFECTIVE: {
                "prefix": "pay_effective",
                "keep": ["uid", "agency", "salary", "salary_freq"],
            },
            events.OFFICER_RANK: {
                "prefix": "rank",
                "keep": ["uid", "agency", "rank_code", "rank_desc"],
            },
            events.OFFICER_DEPT: {
                "prefix": "dept",
                "keep": ["uid", "agency", "department_code", "department_desc"],
                "id_cols": ["uid", "department_code"],
            },
        },
        ["uid"],
    )
    builder.extract_events(
        cprr_18,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.COMPLAINT_INCIDENT: {
                "prefix": "occur",
                "keep": ["uid", "agency", "allegation_uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr_21,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.COMPLAINT_INCIDENT: {
                "prefix": "occur",
                "keep": ["uid", "agency", "allegation_uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr_09,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.COMPLAINT_INCIDENT: {
                "prefix": "occur",
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.OFFICER_LEFT: {
                "prefix": "resignation",
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.OFFICER_LEFT: {
                "prefix": "termination",
                "keep": ["uid", "agency", "allegation_uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        settlements,
        {
            events.CLAIM_RECIEVE: {
                "prefix": "claim_receive",
                "parse_date": True,
                "keep": ["uid", "agency", "settlement_uid"],
            },
            events.CLAIM_OCCUR: {
                "prefix": "claim_occur",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid"],
            },
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
    csd_pprr_17 = pd.read_csv(deba.data("match/pprr_baton_rouge_csd_2017.csv"))
    csd_pprr_19 = pd.read_csv(deba.data("match/pprr_baton_rouge_csd_2019.csv"))
    post_event = pd.read_csv(deba.data("match/event_post_baton_rouge_pd.csv"))
    cprr_18 = pd.read_csv(deba.data("match/cprr_baton_rouge_pd_2018.csv"))
    cprr_21 = pd.read_csv(deba.data("match/cprr_baton_rouge_pd_2021.csv"))
    pprr = pd.read_csv(deba.data("match/pprr_baton_rouge_pd_2021.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    cprr_09 = pd.read_csv(deba.data("match/cprr_baton_rouge_pd_2004_2009.csv"))
    settlements = pd.read_csv(deba.data("match/settlements_baton_rouge_pd_2020.csv"))

    # limit csd data to just officers found in PD roster
    csd_pprr_17.loc[:, "agency"] = "baton-rouge-pd"
    csd_pprr_17 = csd_pprr_17.loc[csd_pprr_17.uid.isin(pprr.uid.unique())].reset_index()
    csd_pprr_19.loc[:, "agency"] = "baton-rouge-pd"
    csd_pprr_19 = csd_pprr_19.loc[csd_pprr_19.uid.isin(pprr.uid.unique())].reset_index()

    uids = pprr.uid.unique().tolist()
    personnel_df = fuse_personnel(
        pprr,
        csd_pprr_17.drop_duplicates(subset="uid", keep="last"),
        csd_pprr_19.drop_duplicates(subset="uid", keep="last"),
        cprr_18,
        cprr_21,
        cprr_09,
        post,
    )

    events_df = fuse_events(
        csd_pprr_17, csd_pprr_19, cprr_18, cprr_21, cprr_09, settlements
    )
    events_df = rearrange_event_columns(pd.concat([post_event, events_df]))
    complaint_df = rearrange_allegation_columns(pd.concat([cprr_18, cprr_21, cprr_09], axis=0))
    settlement_df = rearrange_settlement_columns(settlements)

    personnel_df.to_csv(deba.data("fuse_agency/per_baton_rouge_pd.csv"), index=False)
    events_df.to_csv(deba.data("fuse_agency/event_baton_rouge_pd.csv"), index=False)
    complaint_df.to_csv(deba.data("fuse_agency/com_baton_rouge_pd.csv"), index=False)
    post.to_csv(deba.data("fuse_agency/post_baton_rouge_pd.csv"), index=False)
    settlement_df.to_csv(deba.data("fuse_agency/settlements_baton_rouge_pd.csv"), index=False)
