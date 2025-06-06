import deba
from lib.columns import (
    rearrange_allegation_columns,
)
from lib import events
from lib.personnel import fuse_personnel
from lib.post import load_for_agency
import pandas as pd


def fuse_events(cprr, cprr25, post):
    builder = events.Builder()
    builder.extract_events(
        cprr,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "allegation_uid"],
            }
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr25,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "allegation_uid"],
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
    cprr = pd.read_csv(deba.data("match/cprr_natchitoches_so_2018_21.csv"))
    cppr = cprr.fillna("")
    cprr25 = pd.read_csv(deba.data("match/cprr_natchitoches_so_2022_25.csv"))
    cprr25 = cprr25.fillna("")
    agency = cprr.agency[0] 
    post = load_for_agency(agency)
    per_df = fuse_personnel(cprr, cprr25, post)
    com_df = rearrange_allegation_columns(pd.concat([cprr, cprr25], ignore_index=True))
    event_df = fuse_events(cprr, cprr25, post)
    event_df.to_csv(deba.data("fuse_agency/event_natchitoches_so.csv"), index=False)
    com_df.to_csv(deba.data("fuse_agency/com_natchitoches_so.csv"), index=False)
    per_df.to_csv(deba.data("fuse_agency/per_natchitoches_so.csv"), index=False)
