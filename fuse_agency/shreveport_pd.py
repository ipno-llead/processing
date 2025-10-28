import pandas as pd

import deba
from lib.personnel import fuse_personnel
from lib.columns import rearrange_allegation_columns
from lib.post import load_for_agency
from lib import events


def fuse_events(cprr, cprr21, post, pprr):
    builder = events.Builder()
    builder.extract_events(
        cprr21,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "incident",
                "keep": ["agency", "allegation_uid", "uid"],
            }
        },
        ["allegation_uid"],
    )
    builder.extract_events(
        cprr,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "parse_date": True,
                "keep": ["agency", "allegation_uid", "uid"],
            }
        },
        ["allegation_uid"],
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
                "keep": ["uid", "agency", "rank_desc", "badge_no"],
            },
            events.OFFICER_LEFT: {
                "prefix": "termination",
                "keep": ["uid", "agency", "rank_desc", "badge_no"],
            },
        },
        ["uid", "agency", "rank_desc", "badge_no"],
    )
    return builder.to_frame()



if __name__ == "__main__":
    cprr = pd.read_csv(deba.data("match/cprr_shreveport_pd_2018_2019.csv"))
    pprr = pd.read_csv(deba.data("match/pprr_shreveport_pd_1990_2001.csv"))
    cprr21 = pd.read_csv(deba.data("match/cprr_shreveport_pd_2021_2023.csv"))
    agency = cprr.agency[0]
    cprr = cprr[~((cprr.uid.fillna("") == ""))]
    post = load_for_agency(agency)
    event_df = fuse_events(cprr,cprr21, post, pprr)
    event_df = event_df.drop_duplicates(subset=["event_uid"], keep="first")
    #event_df = fuse_events(cprr, post, pprr)
    per = fuse_personnel(cprr, cprr21, post, pprr)
    cprr25 = pd.concat([cprr, cprr21], ignore_index=True)
    com = rearrange_allegation_columns(cprr25)
    per.to_csv(deba.data("fuse_agency/per_shreveport_pd.csv"), index=False)
    com.to_csv(deba.data("fuse_agency/com_shreveport_pd.csv"), index=False)
    event_df.to_csv(deba.data("fuse_agency/event_shreveport_pd.csv"), index=False)
