import pandas as pd
import deba
from lib.columns import (
    rearrange_appeal_hearing_columns,
    rearrange_allegation_columns,
    rearrange_stop_and_search_columns,
    rearrange_use_of_force,
    rearrange_event_columns,
    rearrange_property_claims_columns,
    rearrange_settlement_columns,
    rearrange_police_report_columns,
    rearrange_citizen_columns,
    rearrange_personnel_columns
)
from lib.clean import float_to_int_str
from lib.personnel import fuse_personnel
from lib import events
from lib.post import load_for_agency
from lib.uid import gen_uid


def fuse_iapro(dfa, dfb):
    dfa = pd.read_csv(deba.data("clean/cprr_new_orleans_pd_1931_2020.csv"))
    dfb = pd.read_csv(deba.data("match/pprr_new_orleans_pd_1946_2018.csv"))
    dfb = dfb.drop(columns=["agency"])

    df = pd.merge(dfa, dfb, on="officer_primary_key")

    df  = df.pipe(gen_uid, ["uid", "tracking_id", "allegation"], "allegation_uid")
    return df


def fuse_events(
    pprr, pprr_csd, cprr, uof, award, lprr, pclaims20, pclaims21, pprr_separations,
    iapro, cprr_venezia, cprr_dillmann, cprr_23, pprr_separations_25
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
                    "overtime_annual_total",
                    "overtime_location",
                    "badge_no",
                    "employee_id",
                    "race",
                    "sex",
                ],
            },
            events.OFFICER_PAY_EFFECTIVE: {
                "prefix": "overtime",
                "parse_date": True,
                "keep": [
                    "uid",
                    "agency",
                    "rank_code",
                    "rank_desc",
                    "salary",
                    "salary_freq",
                    "overtime_annual_total",
                    "overtime_location",
                    "badge_no",
                    "employee_id",
                    "race",
                    "sex",
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
                "keep": [
                    "uid",
                    "agency",
                    "allegation_uid",
                    "years_of_service",
                    "unit_desc",
                    "unit_sub_desc",
                    "division_desc",
                    "department_desc",
                    "employment_status",
                    "race",
                    "sex",
                    "age",
                    "department_desc",
                ],
            },
            events.INVESTIGATION_COMPLETE: {
                "prefix": "investigation_complete",
                "keep": [
                    "uid",
                    "agency",
                    "allegation_uid",
                    "years_of_service",
                    "unit_desc",
                    "unit_sub_desc",
                    "division_desc",
                    "department_desc",
                    "employment_status",
                    "race",
                    "sex",
                    "age",
                    "department_desc",
                ],
            },
            events.COMPLAINT_INCIDENT: {
                "prefix": "occur",
                "keep": [
                    "uid",
                    "agency",
                    "allegation_uid",
                    "years_of_service",
                    "unit_desc",
                    "unit_sub_desc",
                    "division_desc",
                    "department_desc",
                    "employment_status",
                    "race",
                    "sex",
                    "age",
                    "department_desc",
                ],
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
    builder.extract_events(
        iapro,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": [
                    "uid",
                    "agency",
                    "allegation_uid",
                ],
            },
            events.INVESTIGATION_COMPLETE: {
                "prefix": "investigation_complete",
                "keep": [
                    "uid",
                    "agency",
                    "allegation_uid",
                ],
            },
                  events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": [
                    "uid",
                    "agency",
                    "race", 
                    "sex",
                    "department_desc",
                    "badge_no",
                    "rank_desc",
                ],
            },
            events.OFFICER_LEFT: {
                "prefix": "left",
                "keep": [
                    "uid",
                    "agency",
                    "race", 
                    "sex",
                    "department_desc",
                    "badge_no",
                    "rank_desc",
                ],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr_venezia,
        {
            events.COMPLAINT_INCIDENT: {
                "prefix": "incident",
                "parse_date": True,
                "keep": [
                    "uid",
                    "agency",
                    "allegation_uid",
                ],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr_dillmann,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": [
                    "uid",
                    "agency",
                ],
            },
            events.OFFICER_LEFT: {
                "prefix": "left",
                "keep": [
                    "uid",
                    "agency",
                ],
            },
        },
        ["uid"],
    )
    builder.extract_events(
        cprr_23,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": [
                    "uid",
                    "allegation_uid"
                    "agency",
                    "employee_id",
                    "allegation",
                    "allegation_desc",
                    "complainant_type",
                    "tracking_id_og",
                    "tracking_id"
                ],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        pprr_separations_25,
        {
            events.OFFICER_LEFT: {
                "prefix": "left",
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
    pprr_iapro = pd.read_csv(deba.data("match/pprr_new_orleans_pd_1946_2018.csv"))
    cprr_iapro = pd.read_csv(deba.data("clean/cprr_new_orleans_pd_1931_2020.csv"))
    iapro = fuse_iapro(pprr_iapro, cprr_iapro)
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    pprr_csd = pd.read_csv(deba.data("match/pprr_new_orleans_csd_2014.csv"))
    uof = pd.read_csv(deba.data("match/uof_new_orleans_pd_2016_2022.csv"))
    uof = uof.drop_duplicates(subset=["uof_uid"])
    uof_citizen = pd.read_csv(
        deba.data("clean/uof_citizens_new_orleans_pd_2016_2022.csv")
    )
    post_event = pd.read_csv(deba.data("match/post_event_new_orleans_pd.csv"))
    award = pd.read_csv(deba.data("match/award_new_orleans_pd_2016_2021.csv"))
    lprr = pd.read_csv(deba.data("match/lprr_new_orleans_csc_2000_2016.csv"))
    sas = pd.read_csv(deba.data("match/sas_new_orleans_pd_2010_2023.csv"))
    pclaims20 = pd.read_csv(deba.data("match/pclaims_new_orleans_pd_2020.csv"))
    pclaims21 = pd.read_csv(deba.data("match/pclaims_new_orleans_pd_2021.csv"))
    pprr_separations = pd.read_csv(
        deba.data("match/pprr_seps_new_orleans_pd_2018_2022.csv")
    )
    pprr_separations_25 = pd.read_csv(deba.data("match/pprr_seps_new_orleans_pd_2022_2025.csv"))
    cprr = pd.read_csv(deba.data("match/cprr_new_orleans_da_2016_2020.csv"))
    cprr = cprr[~((cprr.uid.fillna("") == ""))]
    # pib = pd.read_csv(deba.data("match/cprr_new_orleans_pib_reports_2014_2020.csv"))
    nopd_settlements = pd.read_csv(
        deba.data("clean/settlements_new_orleans_pd.csv")
    ).dropna()
    pr = pd.read_csv(deba.data("match/pr_new_orleans_pd_2010_2022.csv"))
    cprr_citizens = pd.read_csv(
        deba.data("clean/cprr_cit_new_orleans_da_2016_2020.csv")
    )
    sas_citizens = pd.read_csv(deba.data("clean/sas_cit_new_orleans_pd_2010_2023.csv"))
    pr_citizens = pd.read_csv(deba.data("clean/pr_cit_new_orleans_pd_2010_2022.csv"))
    cprr_venezia = pd.read_csv(deba.data("clean/cprr_new_orleans_pd_venezia.csv"))
    cprr_dillmann = pd.read_csv(deba.data("clean/cprr_new_orleans_pd_dillmann.csv"))
    cprr_23 = pd.read_csv(deba.data("match/cprr_new_orleans_pd_2005_2025.csv"))
    personnel = fuse_personnel(
        pprr,
        lprr,
        pprr_csd,
        uof,
        pclaims20,
        pclaims21,
        pprr_separations,
        cprr,
        post,
        iapro,
        cprr_venezia,
        cprr_dillmann,
        cprr_23,
        pprr_separations_25
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
        iapro,
        cprr_venezia,
        cprr_dillmann,
        cprr_23,
        pprr_separations_25
    )
    events_df = rearrange_event_columns(
        pd.concat([post_event, events_df])
    ).drop_duplicates(subset=["event_uid"], keep="first")
    sas_df = rearrange_stop_and_search_columns(sas)
    lprr_df = rearrange_appeal_hearing_columns(lprr)
    uof_df = rearrange_use_of_force(uof)
    pclaims_df = rearrange_property_claims_columns(pd.concat([pclaims20, pclaims21]))
    com = pd.concat([cprr, iapro, cprr_venezia, cprr_23], axis=0).drop_duplicates(subset=["allegation_uid"], keep="last")
    com = rearrange_allegation_columns(com)
    settlements = rearrange_settlement_columns(nopd_settlements)
    pr = rearrange_police_report_columns(pr)
    personnel = rearrange_personnel_columns(personnel)
    citizen_df =  pd.concat([cprr_citizens, uof_citizen, sas_citizens])
    com.to_csv(deba.data("fuse_agency/com_new_orleans_pd.csv"), index=False)
    personnel.to_csv(deba.data("fuse_agency/per_new_orleans_pd.csv"), index=False)
    events_df.to_csv(deba.data("fuse_agency/event_new_orleans_pd.csv"), index=False)
    lprr_df.to_csv(deba.data("fuse_agency/app_new_orleans_csc.csv"), index=False)
    sas_df.to_csv(deba.data("fuse_agency/sas_new_orleans_pd.csv"), index=False)
    uof_df.to_csv(deba.data("fuse_agency/uof_new_orleans_pd.csv"), index=False)
    pclaims_df.to_csv(deba.data("fuse_agency/pclaims_new_orleans_pd.csv"), index=False)
    settlements.to_csv(deba.data("fuse_agency/settlements_new_orleans_pd.csv"), index=False)
    citizen_df.to_csv(deba.data("fuse_agency/cit_new_orleans_pd.csv"), index=False)
    pr.to_csv(deba.data("fuse_agency/pr_new_orleans_pd.csv"), index=False)
