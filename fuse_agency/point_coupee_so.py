import pandas as pd
import deba
from lib.columns import (
    rearrange_allegation_columns,
    rearrange_personnel_columns,
    rearrange_event_columns,
)
from lib import events
from lib.personnel import fuse_personnel
from lib.post import load_for_agency


def fuse_events(cprr, post):
    builder = events.Builder()
    builder.extract_events(
        cprr,
        {
            events.OFFICER_LEFT: {
                "prefix": "termination",
                "parse_date": True,
                "keep": [
                    "uid",
                    "agency",
                    "allegation_uid",
                    "tracking_id",
                ],
            },
            events.OFFICER_LEFT: {
                "prefix": "arrest",
                "parse_date": True,
                "keep": [
                    "uid",
                    "agency",
                    "allegation_uid",
                    "tracking_id",
                ],
            },
        },
        ["allegation_uid", "uid"],
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
    cprr = pd.read_csv(deba.data("match/cprr_point_coupee_so_2017.csv"))
    agency = cprr.agency[0]
    post = load_for_agency(agency)

    events_df = fuse_events(cprr, post)
    cprr_df = rearrange_allegation_columns(cprr)
    personnel = fuse_personnel(post, cprr)
    personnel = rearrange_personnel_columns(personnel)

    events_df.to_csv(deba.data("fuse_agency/event_point_coupee_so.csv"), index=False)
    personnel.to_csv(deba.data("fuse_agency/per_point_coupee_so.csv"), index=False)
    cprr_df.to_csv(deba.data("fuse_agency/com_point_coupee_so.csv"), index=False)
