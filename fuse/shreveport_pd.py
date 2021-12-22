import sys

import pandas as pd

from lib.path import data_file_path, ensure_data_dir
from lib.personnel import fuse_personnel
from lib.columns import rearrange_allegation_columns
from lib import events

sys.path.append("../")


def fuse_events(cprr, post):
    builder = events.Builder()
    builder.extract_events(
        cprr,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "parse_date": True,
                "keep": ["agency", "allegation_uid", "uid"],
            }
        },
        ["allegation_uid"],
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
    cprr = pd.read_csv(data_file_path("match/cprr_shreveport_pd_2018_2019.csv"))
    post = pd.read_csv(data_file_path("clean/pprr_post_2020_11_06.csv"))
    post = post[post.agency == "shreveport pd"]
    event_df = fuse_events(cprr, post)
    per = fuse_personnel(cprr, post)
    com = rearrange_allegation_columns(cprr)
    per.to_csv(data_file_path("fuse/per_shreveport_pd.csv"), index=False)
    com.to_csv(data_file_path("fuse/com_shreveport_pd.csv"), index=False)
    event_df.to_csv(data_file_path("fuse/event_shreveport_pd.csv"), index=False)
