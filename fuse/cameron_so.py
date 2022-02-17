import pandas as pd
import dirk
from lib import events
from lib.columns import rearrange_allegation_columns
from lib.personnel import fuse_personnel
from lib.post import load_for_agency


def fuse_events(post):
    builder = events.Builder()
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
    cprr_20 = pd.read_csv(dirk.data("match/cprr_cameron_so_2020.csv"))
    cprr_19 = pd.read_csv(dirk.data("match/cprr_cameron_so_2015_2019.csv"))
    agency = cprr_20.agency[0]
    post = load_for_agency(agency)
    per = fuse_personnel(cprr_20, cprr_19, post)
    com = rearrange_allegation_columns(pd.concat([cprr_20, cprr_19]))
    event = fuse_events(post)
    event.to_csv(dirk.data("fuse/event_cameron_so.csv"), index=False)
    com.to_csv(dirk.data("fuse/com_cameron_so.csv"), index=False)
    per.to_csv(dirk.data("fuse/per_cameron_so.csv"), index=False)
