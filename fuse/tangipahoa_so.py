import dirk
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
            events.INVESTIGATION_COMPLETE: {
                "prefix": "completion",
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
    cprr = pd.read_csv(dirk.data("match/cprr_tangipahoa_so_2015_2021.csv"))
    agency = cprr.agency[0]
    post = load_for_agency(agency)
    per = fuse_personnel(cprr, post)
    complaints = rearrange_allegation_columns(cprr)
    event = fuse_events(cprr, post)
    event.to_csv(dirk.data("fuse/event_tangipahoa_so.csv"), index=False)
    complaints.to_csv(dirk.data("fuse/com_tangipahoa_so.csv"), index=False)
    per.to_csv(dirk.data("fuse/per_tangipahoa_so.csv"), index=False)
