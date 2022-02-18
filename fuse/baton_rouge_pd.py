import pandas as pd
import bolo
from lib.personnel import fuse_personnel
from lib.columns import (
    rearrange_event_columns,
    rearrange_allegation_columns,
    rearrange_appeal_hearing_columns,
)
from lib import events


def fuse_events(csd_pprr_17, csd_pprr_19, cprr_18, cprr_21, lprr):
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
        lprr,
        {
            events.APPEAL_HEARING: {
                "prefix": "appeal_hearing",
                "keep": ["uid", "agency", "appeal_uid"],
            },
            events.APPEAL_DISPOSITION: {
                "prefix": "appeal_disposition",
                "keep": ["uid", "agency", "appeal_uid", "disposition_uid"],
            },
        },
        ["uid", "appeal_uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    csd_pprr_17 = pd.read_csv(bolo.data("match/pprr_baton_rouge_csd_2017.csv"))
    csd_pprr_19 = pd.read_csv(bolo.data("match/pprr_baton_rouge_csd_2019.csv"))
    lprr = pd.read_csv(bolo.data("match/lprr_baton_rouge_fpcsb_1992_2012.csv"))
    post_event = pd.read_csv(bolo.data("match/event_post_baton_rouge_pd.csv"))
    cprr_18 = pd.read_csv(bolo.data("match/cprr_baton_rouge_pd_2018.csv"))
    cprr_21 = pd.read_csv(bolo.data("match/cprr_baton_rouge_pd_2021.csv"))
    pprr = pd.read_csv(bolo.data("clean/pprr_baton_rouge_pd_2021.csv"))

    # limit csd data to just officers found in PD roster
    csd_pprr_17.loc[:, "agency"] = "Baton Rouge PD"
    csd_pprr_17 = csd_pprr_17.loc[csd_pprr_17.uid.isin(pprr.uid.unique())].reset_index()
    csd_pprr_19.loc[:, "agency"] = "Baton Rouge PD"
    csd_pprr_19 = csd_pprr_19.loc[csd_pprr_19.uid.isin(pprr.uid.unique())].reset_index()

    uids = pprr.uid.unique().tolist()
    personnel_df = fuse_personnel(
        pprr,
        csd_pprr_17.drop_duplicates(subset="uid", keep="last"),
        csd_pprr_19.drop_duplicates(subset="uid", keep="last"),
        cprr_18,
        cprr_21,
        lprr,
    )

    events_df = fuse_events(csd_pprr_17, csd_pprr_19, cprr_18, cprr_21, lprr)
    events_df = rearrange_event_columns(pd.concat([post_event, events_df]))
    complaint_df = rearrange_allegation_columns(pd.concat([cprr_18, cprr_21]))
    lprr_df = rearrange_appeal_hearing_columns(lprr)

    personnel_df.to_csv(bolo.data("fuse/per_baton_rouge_pd.csv"), index=False)
    events_df.to_csv(bolo.data("fuse/event_baton_rouge_pd.csv"), index=False)
    complaint_df.to_csv(bolo.data("fuse/com_baton_rouge_pd.csv"), index=False)
    lprr_df.to_csv(bolo.data("fuse/app_baton_rouge_pd.csv"), index=False)
