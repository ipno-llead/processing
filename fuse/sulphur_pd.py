import deba
from lib.columns import (
    rearrange_allegation_columns,
)
from lib import events
from lib.personnel import fuse_personnel
from lib.post import load_for_agency
import pandas as pd


def fuse_events(cprr, post):
    builder = events.Builder()
    builder.extract_events(
        cprr,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.DISPOSITION: {
                "prefix": "disposition",
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
    cprr = pd.read_csv(deba.data("match/cprr_sulphur_pd_2014_2019.csv"))
    agency = cprr.agency[0]
    post = load_for_agency(agency)
    per_df = fuse_personnel(cprr, post)
    com_df = rearrange_allegation_columns(cprr)
    event_df = fuse_events(cprr, post)
    event_df.to_csv(deba.data("fuse/event_sulphur_pd.csv"), index=False)
    com_df.to_csv(deba.data("fuse/com_sulphur_pd.csv"), index=False)
    per_df.to_csv(deba.data("fuse/per_sulphur_pd.csv"), index=False)
