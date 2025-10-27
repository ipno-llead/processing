import deba
from lib.columns import (
    rearrange_allegation_columns,
    rearrange_appeal_hearing_columns,
    rearrange_event_columns,
    rearrange_settlement_columns,
    rearrange_docs_columns,
    rearrange_use_of_force,
)
from lib.personnel import fuse_personnel
from lib import events
from lib.post import load_for_agency
import pandas as pd


def fuse_events(lprr, pprr, pprr_term, settlements, cprr, cprr22, uof):
    builder = events.Builder()
    builder.extract_events(
        lprr,
        {
            events.APPEAL_FILE: {
                "prefix": "filed",
                "keep": ["uid", "agency", "appeal_uid"],
            },
            events.APPEAL_DISPOSITION: {
                "prefix": "appeal_disposition",
                "keep": ["uid", "agency", "appeal_uid"],
            },
        },
        ["uid", "appeal_uid"],
    )
    pprr.loc[:, "salary_date"] = pprr.salary_date.astype(str)
    builder.extract_events(
        pprr,
        {
            events.OFFICER_HIRE: {"prefix": "hire", "keep": ["uid", "agency"]},
            events.OFFICER_PAY_EFFECTIVE: {
                "prefix": "salary",
                "parse_date": "%Y%m%d",
                "keep": [
                    "uid",
                    "agency",
                    "salary",
                    "salary_freq",
                    "department_desc",
                    "rank_desc",
                ],
            },
        },
        ["uid"],
    )
    builder.extract_events(
        pprr_term,
        {
            events.OFFICER_LEFT: {
                "prefix": "left",
                "keep": [
                    "uid",
                    "agency",
                    "department_desc",
                    "rank_desc",
                    "left_reason",
                ],
            }
        },
        ["uid"],
    )
    builder.extract_events(
        settlements,
        {
            events.SETTLEMENT_CHECK: {
                "prefix": "check",
                "parse_date": True,
                "keep": [
                    "uid",
                    "settlement_uid",
                    "agency",
                ],
            }
        },
        ["uid", "settlement_uid"],
    )
    builder.extract_events(
        cprr,
        {
            events.COMPLAINT_INCIDENT: {
                "prefix": "incident",
                "parse_date": True,
                "keep": [
                    "uid",
                    "allegation_uid",
                    "agency",
                ],
            },
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "parse_date": True,
                "keep": [
                    "uid",
                    "allegation_uid",
                    "agency",
                ],
            },
            events.OFFICER_LEFT: {
                "prefix": "termination",
                "parse_date": True,
                "keep": [
                    "uid",
                    "allegation_uid",
                    "agency",
                ],
            },
            events.OFFICER_LEFT: {
                "prefix": "separation",
                "parse_date": True,
                "keep": [
                    "uid",
                    "allegation_uid",
                    "agency",
                ],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr22,
        {
            events.COMPLAINT_INCIDENT: {
                "prefix": "incident",
                "keep": [
                    "uid",
                    "allegation_uid",
                    "agency",
                ],
            },
            events.OFFICER_LEFT: {
                "prefix": "termination",
                "keep": [
                    "uid",
                    "allegation_uid",
                    "agency",
                ],
            },
            events.OFFICER_DEMOTION: {
                "prefix": "letter_of_reprimand",
                "keep": [
                    "uid",
                    "allegation_uid",
                    "agency",
                ],
            },
            events.SUSPENSION_START: {
                "prefix": "suspension",
                "keep": [
                    "uid",
                    "allegation_uid",
                    "agency",
                ],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        uof,
        {
            events.UOF_INCIDENT: {
                "prefix": "incident",
                "keep": ["uid", "uof_uid", "agency"],
            },
        },
        ["uid", "uof_uid"],
    )
    return builder.to_frame()

#

if __name__ == "__main__":
    lprr = pd.read_csv(deba.data("clean/lprr_louisiana_state_csc_1991_2020.csv"))
    pprr = pd.read_csv(deba.data("clean/pprr_demo_louisiana_csd_2021.csv"))
    pprr_term = pd.read_csv(deba.data("clean/pprr_term_louisiana_csd_2021.csv"))
    settlements = pd.read_csv(
        deba.data("match/settlements_louisiana_state_pd_2010_2022.csv")
    )
    cprr = pd.read_csv(deba.data("match/cprr_louisiana_state_pd_2019_2020.csv"))
    cprr22 = pd.read_csv(deba.data("match/cprr_louisiana_state_pd_2021_2022.csv"))
    post_event = pd.read_csv(deba.data("match/post_event_louisiana_state_police_2020.csv"))
    uof = pd.read_csv(deba.data("match/uof_louisiana_state_pd_2022.csv"))
    #uof = uof.drop_duplicates(subset=["uof_uid"])
    uof_citizen = pd.read_csv(
        deba.data("clean/uof_cit_louisiana_state_pd_2022.csv")
    )
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    per_df = fuse_personnel(pprr, pprr_term, lprr, post, cprr, cprr22, uof)
    per_df = per_df[~((per_df.last_name.fillna("") == ""))]
    event_df = rearrange_event_columns(fuse_events(lprr, pprr, pprr_term, settlements, cprr, cprr22, uof))
    post_event = rearrange_event_columns(post_event)
    event_df = pd.concat([event_df, post_event])
    settlements = rearrange_settlement_columns(settlements)
    app_df = rearrange_appeal_hearing_columns(lprr)
    combined_cprr = pd.concat([cprr, cprr22])
    com = rearrange_allegation_columns(combined_cprr)
    uof = rearrange_use_of_force(uof)
    per_df.to_csv(deba.data("fuse_agency/per_louisiana_state_pd.csv"), index=False)
    event_df.to_csv(deba.data("fuse_agency/event_louisiana_state_pd.csv"), index=False)
    app_df.to_csv(deba.data("fuse_agency/app_louisiana_state_pd.csv"), index=False)
    settlements.to_csv(
        deba.data("fuse_agency/settlements_louisiana_state_pd.csv"), index=False
    )
    uof.to_csv(deba.data("fuse_agency/uof_louisiana_state_pd.csv"), index=False)
    com.to_csv(deba.data("fuse_agency/com_louisiana_state_pd.csv"), index=False)
    post.to_csv(deba.data("fuse_agency/post_louisiana_state_pd.csv"), index=False)
    uof_citizen.to_csv(
        deba.data("fuse_agency/cit_louisiana_state_pd.csv.csv"), index=False
    )
