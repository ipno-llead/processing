import deba
from lib.columns import (
    rearrange_allegation_columns,
    rearrange_appeal_hearing_columns,
    rearrange_event_columns,
    rearrange_settlement_columns,
)
from lib.personnel import fuse_personnel
from lib import events
from lib.post import load_for_agency
import pandas as pd


def fuse_events(lprr, pprr, pprr_term, cprr19, cprr20):
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
        cprr19,
        {
            events.INVESTIGATION_COMPLETE: {
                "prefix": "letter",
                "parse_date": True,
                "keep": ["uid", "rank_desc", "agency", "letter_subject", "tracking_id"],
            }
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr20,
        {
            events.INVESTIGATION_COMPLETE: {
                "prefix": "report",
                "keep": [
                    "uid",
                    "agency",
                    "report_subject",
                    "allegation",
                    "disposition",
                ],
            }
        },
        ["uid", "allegation_uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    lprr = pd.read_csv(deba.data("match/lprr_louisiana_state_csc_1991_2020.csv"))
    pprr = pd.read_csv(deba.data("clean/pprr_demo_louisiana_csd_2021.csv"))
    pprr_term = pd.read_csv(deba.data("clean/pprr_term_louisiana_csd_2021.csv"))
    post_event = pd.read_csv(
        deba.data("match/post_event_louisiana_state_police_2020.csv")
    )
    settlements = pd.read_csv(
        deba.data("clean/settlements_louisiana_state_pd_2015_2020.csv")
    )
    cprr19 = pd.read_csv(deba.data("match/cprr_louisiana_state_pd_2019.csv"))
    cprr20 = pd.read_csv(deba.data("match/cprr_louisiana_state_pd_2020.csv"))
    agency = cprr19.agency[0]
    post = load_for_agency(agency)
    per_df = fuse_personnel(pprr, pprr_term, lprr, cprr19, cprr20, post)
    per_df = per_df[~((per_df.last_name.fillna("") == ""))]
    event_df = rearrange_event_columns(
        pd.concat([post_event, fuse_events(lprr, pprr, pprr_term, cprr19, cprr20)])
    )
    settlements = rearrange_settlement_columns(settlements)
    app_df = rearrange_appeal_hearing_columns(lprr)
    cprr = rearrange_allegation_columns(pd.concat([cprr19, cprr20]))
    per_df.to_csv(deba.data("fuse/per_louisiana_state_pd.csv"), index=False)
    event_df.to_csv(deba.data("fuse/event_louisiana_state_pd.csv"), index=False)
    app_df.to_csv(deba.data("fuse/app_louisiana_state_pd.csv"), index=False)
    settlements.to_csv(
        deba.data("fuse/settlements_louisiana_state_pd.csv"), index=False
    )
    cprr.to_csv(deba.data("fuse/com_louisiana_state_pd.csv"), index=False)
    post.to_csv(deba.data("fuse/post_louisiana_state_pd.csv"), index=False)
