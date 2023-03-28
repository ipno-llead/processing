from lib.columns import (
    rearrange_allegation_columns,
    rearrange_personnel_columns,
)
import deba
from lib.personnel import fuse_personnel
from lib.post import load_for_agency
from lib import events
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
    return builder.to_frame()


if __name__ == "__main__":
    cprr = pd.read_csv(deba.data("match/iberia_so_cprr_2019_2021.csv"))
    agency = cprr.agency[0]
    post = load_for_agency(agency)
    per = rearrange_personnel_columns(cprr)
    per = fuse_personnel(per)
    com = rearrange_allegation_columns(cprr)
    event = fuse_events(cprr, post)
    per.to_csv(deba.data("fuse_agency/per_iberia_so.csv"), index=False)
    com.to_csv(deba.data("fuse_agency/com_iberia_so.csv"), index=False)
    event.to_csv(deba.data("fuse_agency/event_iberia_so.csv"), index=False)
