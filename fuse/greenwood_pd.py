from lib.columns import rearrange_allegation_columns, rearrange_personnel_columns
from lib.path import data_file_path
from lib import events
import pandas as pd
import sys

sys.path.append("../")


def fuse_events(cprr, post):
    builder = events.Builder()
    builder.extract_events(
        cprr,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.COMPLAINT_INCIDENT: {
                "prefix": "occur",
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
    cprr = pd.read_csv(data_file_path('clean/cprr_greenwood_pd_2015_2020.csv'))
    post = pd.read_csv(data_file_path("clean/pprr_post_2020_11_06.csv"))
    per = rearrange_personnel_columns(post)
    com = rearrange_allegation_columns(cprr)
    event = fuse_events(cprr, post)
    per.to_csv(data_file_path("fuse/per_greenwood_pd.csv"), index=False)
    com.to_csv(data_file_path("fuse/com_greenwood_pd.csv"), index=False)
    event.to_csv(data_file_path("fuse/event_greenwood_pd.csv"), index=False)
