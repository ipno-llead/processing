from lib.columns import (
    rearrange_allegation_columns,
    rearrange_personnel_columns,
    rearrange_citizen_columns,
)
import deba
from lib.personnel import fuse_personnel
from lib.post import load_for_agency
from lib import events
import pandas as pd


def fuse_events(cprr, post, pprr):
    builder = events.Builder()
    builder.extract_events(
        cprr,
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
        pprr,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": ["uid", "agency", "rank", "badge_no", "salary", "salary_freq"],
            },
        },
        ["uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr = pd.read_csv(deba.data("match/cprr_greenwood_pd_2015_2020.csv"))
    pprr = pd.read_csv(deba.data("match/pprr_greenwood_pd_1990_2001.csv"))
    citizen_df = pd.read_csv(deba.data("clean/cprr_cit_greenwood_pd_2015_2020.csv"))
    agency = cprr.agency[0]
    post = load_for_agency(agency)
    per_pprr = rearrange_personnel_columns(pprr)
    per_post = rearrange_personnel_columns(post)
    per = fuse_personnel(per_pprr, per_post)
    com = rearrange_allegation_columns(cprr)
    event = fuse_events(cprr, post, pprr)
    citizen_df = rearrange_citizen_columns(citizen_df)
    per.to_csv(deba.data("fuse_agency/per_greenwood_pd.csv"), index=False)
    com.to_csv(deba.data("fuse_agency/com_greenwood_pd.csv"), index=False)
    event.to_csv(deba.data("fuse_agency/event_greenwood_pd.csv"), index=False)
    post.to_csv(deba.data("fuse_agency/post_greenwood_pd.csv"), index=False)
    citizen_df.to_csv(deba.data("fuse_agency/cit_greenwood_pd.csv"), index=False)
