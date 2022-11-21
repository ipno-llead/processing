import deba
from lib.columns import rearrange_allegation_columns
from lib.personnel import fuse_personnel
from lib.post import load_for_agency
import pandas as pd
from lib import events


def fuse_events(cprr21, post, cprr14, cprr04):
    builder = events.Builder()
    builder.extract_events(
        cprr21,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": [
                    "uid",
                    "allegation_uid",
                    "agency",
                    "allegation_desc",
                    "action",
                ],
            }
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
        cprr14,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": [
                    "uid",
                    "allegation_uid",
                    "agency",
                    "allegation_desc",
                    "action",
                ],
            },
        },
        ["uid"],
    )
    builder.extract_events(
        cprr04,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": [
                    "uid",
                    "allegation_uid",
                    "agency",
                    "allegation_desc",
                    "action",
                ],
            },
            events.OFFICER_LEFT: {
                "prefix": "resignation",
                "keep": [
                    "uid",
                    "allegation_uid",
                    "agency",
                    "allegation_desc",
                    "action",
                ],
            },
        },
        ["uid"],
    )

    return builder.to_frame()


if __name__ == "__main__":
    cprr21 = pd.read_csv(deba.data("match/cprr_benton_pd_2015_2021.csv"))
    cprr14 = pd.read_csv(deba.data("match/cprr_benton_pd_2009_2014.csv"))
    cprr04 = pd.read_csv(deba.data("match/cprr_benton_pd_2004_2008.csv"))
    agency = cprr21.agency[0]
    post = load_for_agency(agency)
    per = fuse_personnel(cprr21, post, cprr14, cprr04)
    complaints = rearrange_allegation_columns(pd.concat([cprr21, cprr14]))
    event = fuse_events(cprr21, post, cprr14, cprr04)
    complaints.to_csv(deba.data("fuse/com_benton_pd.csv"), index=False)
    per.to_csv(deba.data("fuse/per_benton_pd.csv"), index=False)
    event.to_csv(deba.data("fuse/event_benton_pd.csv"), index=False)
