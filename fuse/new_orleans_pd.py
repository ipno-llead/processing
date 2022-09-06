import pandas as pd
import deba
from lib.columns import (
    rearrange_appeal_hearing_columns,
    rearrange_allegation_columns,
    rearrange_brady_columns,
    rearrange_uof_citizen_columns,
    rearrange_stop_and_search_columns,
    rearrange_use_of_force,
    rearrange_event_columns,
    rearrange_uof_officer_columns,
    rearrange_property_claims_columns,
)
from lib.clean import float_to_int_str
from lib.personnel import fuse_personnel
from lib import events


def fuse_events(
    pprr, pprr_csd, cprr, uof, award, lprr, pclaims20, pclaims21, pprr_separations
):
    builder = events.Builder()
    builder.extract_events(
        pprr,
        {
            events.OFFICER_PAY_EFFECTIVE: {
                "prefix": "salary",
                "parse_date": True,
                "keep": [
                    "uid",
                    "agency",
                    "rank_desc",
                    "salary",
                    "salary_freq",
                    "overtime_and_detail_annual_total",
                ],
            },
            events.OFFICER_PAY_EFFECTIVE: {
                "prefix": "overtime_and_detail",
                "parse_date": True,
                "keep": [
                    "uid",
                    "agency",
                    "rank_code",
                    "rank_desc",
                    "salary",
                    "salary_freq",
                    "overtime_and_detail_annual_total",
                ],
            },
        },
        ["uid"],
    )
    builder.extract_events(
        pprr_csd,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": [
                    "uid",
                    "agency",
                    "rank_code",
                    "rank_desc",
                    "salary",
                    "salary_freq",
                ],
            },
            events.OFFICER_LEFT: {
                "prefix": "term",
                "keep": [
                    "uid",
                    "agency",
                    "rank_code",
                    "rank_desc",
                    "salary",
                    "salary_freq",
                ],
            },
            events.OFFICER_PAY_PROG_START: {"prefix": "pay_prog_start"},
        },
        ["uid"],
        warn_duplications=True,
    )
    builder.extract_events(
        cprr,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.INVESTIGATION_COMPLETE: {
                "prefix": "investigation_complete",
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
        uof,
        {
            events.UOF_INCIDENT: {
                "prefix": "uof_occur",
                "keep": ["uid", "uof_uid", "agency"],
            },
        },
        ["uid", "uof_uid"],
    )
    builder.extract_events(
        award,
        {
            events.AWARD_RECEIVE: {
                "prefix": "receive",
                "parse_date": True,
                "keep": ["uid", "agency", "award"],
            },
            events.AWARD_RECOMMENDED: {
                "prefix": "recommendation",
                "parse_date": True,
                "keep": ["uid", "agency", "recommended_award"],
            },
        },
        ["uid", "award"],
    )
    builder.extract_events(
        lprr,
        {
            events.APPEAL_DISPOSITION: {
                "prefix": "appeal_disposition",
                "keep": ["uid", "agency", "appeal_uid"],
            },
            events.APPEAL_RECEIVE: {
                "prefix": "appeal_receive",
                "keep": ["uid", "agency", "appeal_uid"],
            },
            events.APPEAL_HEARING: {
                "prefix": "appeal_hearing",
                "keep": ["uid", "agency", "appeal_uid"],
            },
        },
        ["uid", "appeal_uid"],
    )
    builder.extract_events(
        pclaims20,
        {
            events.CLAIM_MADE: {
                "prefix": "claim_made",
                "keep": ["uid", "agency", "property_claim_uid"],
            },
            events.CLAIM_RECIEVE: {
                "prefix": "claim_receive",
                "keep": ["uid", "agency", "property_claim_uid"],
            },
            events.CLAIM_CLOSED: {
                "prefix": "claim_close",
                "keep": ["uid", "agency", "property_claim_uid"],
            },
            events.CLAIM_OCCUR: {
                "prefix": "claim_occur",
                "keep": ["uid", "agency", "property_claim_uid"],
            },
        },
        ["uid", "property_claims_uid"],
    )
    builder.extract_events(
        pclaims21,
        {
            events.CLAIM_MADE: {
                "prefix": "claim_made",
                "parse_date": True,
                "keep": ["uid", "agency", "property_claim_uid"],
            },
            events.CLAIM_RECIEVE: {
                "prefix": "claim_receive",
                "parse_date": True,
                "keep": ["uid", "agency", "property_claim_uid"],
            },
            events.CLAIM_CLOSED: {
                "prefix": "claim_close",
                "parse_date": True,
                "keep": ["uid", "agency", "property_claim_uid"],
            },
            events.CLAIM_OCCUR: {
                "prefix": "claim_occur",
                "parse_date": True,
                "keep": ["uid", "agency", "property_claim_uid"],
            },
        },
        ["uid", "property_claims_uid"],
    )
    builder.extract_events(
        pprr_separations,
        {
            events.OFFICER_LEFT: {
                "prefix": "left",
                "parse_date": True,
                "keep": [
                    "uid",
                    "agency",
                    "left_reason",
                    "left_reason_desc",
                    "years_of_service",
                ],
            },
        },
        ["uid", "separation_uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    pprr = pd.read_csv(deba.data("clean/pprr_new_orleans_pd_2020.csv"))
    pprr_csd = pd.read_csv(deba.data("match/pprr_new_orleans_csd_2014.csv"))
    uof_officers = pd.read_csv(
        deba.data("clean/uof_officers_new_orleans_pd_2016_2021.csv")
    )
    uof_citizens = pd.read_csv(
        deba.data("clean/uof_citizens_new_orleans_pd_2016_2021.csv")
    )
    uof = pd.read_csv(deba.data("clean/uof_new_orleans_pd_2016_2021.csv"))
    post_event = pd.read_csv(deba.data("match/post_event_new_orleans_pd.csv"))
    award = pd.read_csv(deba.data("match/award_new_orleans_pd_2016_2021.csv"))
    lprr = pd.read_csv(deba.data("match/lprr_new_orleans_csc_2000_2016.csv"))
    sas = pd.read_csv(deba.data("match/sas_new_orleans_pd_2017_2021.csv"))
    brady = pd.read_csv(deba.data("match/brady_new_orleans_da_2021.csv"))
    brady = brady.loc[brady.agency == "New Orleans PD"]
    pclaims20 = pd.read_csv(deba.data("match/pclaims_new_orleans_pd_2020.csv"))
    pclaims21 = pd.read_csv(deba.data("match/pclaims_new_orleans_pd_2021.csv"))
    pprr_separations = pd.read_csv(
        deba.data("match/pprr_seps_new_orleans_pd_2018_2022.csv")
    )

    cprr = pd.read_csv(deba.data("match/cprr_new_orleans_da_2016_2020.csv"))
    pib = pd.read_csv(deba.data("match/cprr_new_orleans_pib_reports_2014_2020.csv"))
    personnel = fuse_personnel(
        pprr,
        lprr,
        pprr_csd,
        uof_officers,
        brady,
        pclaims20,
        pclaims21,
        pprr_separations,
        cprr,
        pib,
    )
    events_df = fuse_events(
        pprr,
        pprr_csd,
        cprr,
        uof,
        award,
        lprr,
        pclaims20,
        pclaims21,
        pprr_separations,
    )
    events_df = rearrange_event_columns(
        pd.concat([post_event, events_df])
    ).drop_duplicates(subset=["event_uid"], keep="first")
    sas_df = rearrange_stop_and_search_columns(sas)
    lprr_df = rearrange_appeal_hearing_columns(lprr)
    uof_officer_df = rearrange_uof_officer_columns(uof_officers)
    uof_citizen_df = rearrange_uof_citizen_columns(uof_citizens)
    uof_df = rearrange_use_of_force(uof)
    brady_df = rearrange_brady_columns(brady)
    pclaims_df = rearrange_property_claims_columns(pd.concat([pclaims20, pclaims21]))
    com = pd.concat([cprr, pib]).drop_duplicates(subset=["allegation_uid"], keep="last")
    com = rearrange_allegation_columns(com)
    com.to_csv(deba.data("fuse/com_new_orleans_pd.csv"), index=False)
    personnel.to_csv(deba.data("fuse/per_new_orleans_pd.csv"), index=False)
    events_df.to_csv(deba.data("fuse/event_new_orleans_pd.csv"), index=False)
    lprr_df.to_csv(deba.data("fuse/app_new_orleans_csc.csv"), index=False)
    sas_df.to_csv(deba.data("fuse/sas_new_orleans_pd.csv"), index=False)
    uof_df.to_csv(deba.data("fuse/uof_new_orleans_pd.csv"), index=False)
    uof_officer_df.to_csv(
        deba.data("fuse/uof_officers_new_orleans_pd.csv"), index=False
    )
    uof_citizen_df.to_csv(
        deba.data("fuse/uof_citizens_new_orleans_pd.csv"), index=False
    )
    brady_df.to_csv(deba.data("fuse/brady_new_orleans_pd.csv"), index=False)
    pclaims_df.to_csv(deba.data("fuse/pclaims_new_orleans_pd.csv"), index=False)
