import deba
from lib.columns import rearrange_allegation_columns
from lib.personnel import fuse_personnel
from lib.post import load_for_agency
import pandas as pd
from lib import events


def fuse_events(cprr, post):
    builder = events.Builder()
    builder.extract_events(
        cprr,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation", "action"],
            },
            events.COMPLAINT_INCIDENT: {
                "prefix": "incident",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation", "action"],
            },
            events.OFFICER_LEFT: {
                "prefix": "termination",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation", "action"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        post,
        {
            events.OFFICER_LEVEL_1_CERT: {
                "prefix": "level_1_cert",
                "parse_date": True,
                "keep": ["uid", "agency"],
            },
            events.OFFICER_PC_12_QUALIFICATION: {
                "prefix": "last_pc_12_qualification",
                "parse_date": True,
                "keep": ["uid", "agency"],
            },
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "parse_date": True,
                "keep": ["uid", "agency"],
            },
        },
        ["uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr = pd.read_csv(deba.data("match/cprr_lasalle_so_2018_2022.csv"))
    post = pd.read_csv(deba.data("clean/pprr_post_4_26_2023.csv"))
    post = post[post.agency.isin(["lasalle-so"])]
    per = fuse_personnel(cprr, post)
    complaints = rearrange_allegation_columns(cprr)
    event = fuse_events(cprr, post)
    complaints.to_csv(deba.data("fuse_agency/com_lasalle_so.csv"), index=False)
    per.to_csv(deba.data("fuse_agency/per_lasalle_so.csv"), index=False)
    event.to_csv(deba.data("fuse_agency/event_lasalle_so.csv"), index=False)
