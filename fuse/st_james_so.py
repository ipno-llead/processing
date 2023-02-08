import deba
from lib.columns import (
    rearrange_allegation_columns,
)
from lib import events
from lib.personnel import fuse_personnel
from lib.post import load_for_agency
import pandas as pd


def fuse_events(cprr, cprr00, post):
    builder = events.Builder()
    builder.extract_events(
        cprr,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "allegation_uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr00,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "parse_date": True,
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
                "keep": ["uid", "agency", "employment_status"],
            },
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": ["uid", "agency", "employment_status"],
            },
            events.OFFICER_PC_12_QUALIFICATION: {
                "prefix": "last_pc_12_qualification",
                "parse_date": "%Y-%m-%d",
                "keep": [
                    "uid",
                    "agency",
                    "employment_status",
                ],
            },
        },
        ["uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr = pd.read_csv(deba.data("match/cprr_st_james_so_2019_2021.csv"))
    cprr00 = pd.read_csv(deba.data("match/cprr_st_james_so_1990_2000.csv"))
    agency = cprr.agency[0]
    post = load_for_agency(agency)
    per_df = fuse_personnel(cprr, cprr00, post)
    com_df = rearrange_allegation_columns(pd.concat([cprr, cprr00], axis=0))
    event_df = fuse_events(cprr, cprr00, post)
    event_df.to_csv(deba.data("fuse/event_st_james_so.csv"), index=False)
    com_df.to_csv(deba.data("fuse/com_st_james_so.csv"), index=False)
    per_df.to_csv(deba.data("fuse/per_st_james_so.csv"), index=False)
