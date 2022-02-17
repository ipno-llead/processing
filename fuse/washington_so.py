import pandas as pd
import dirk
from lib import events
from lib.columns import rearrange_allegation_columns
from lib.personnel import fuse_personnel
from lib.post import load_for_agency


def fuse_events(cprr, post):
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
                "keep": ["uid", "agency", "employment_status"],
            },
        },
        ["uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr = pd.read_csv(dirk.data("clean/cprr_washington_so_2015_2020.csv"))
    agency = cprr.agency[0]
    post = load_for_agency(agency)
    per = fuse_personnel(cprr, post)
    com = rearrange_allegation_columns(cprr)
    event = fuse_events(cprr, post)
    event.to_csv(dirk.data("fuse/event_washington_so.csv"), index=False)
    com.to_csv(dirk.data("fuse/com_washington_so.csv"), index=False)
    per.to_csv(dirk.data("fuse/per_washington_so.csv"), index=False)
