import sys

sys.path.append("../")
from lib.path import data_file_path
from lib.columns import (
    rearrange_allegation_columns,
)
from lib import events
from lib.personnel import fuse_personnel
import pandas as pd


def prepare_post():
    post = pd.read_csv(data_file_path("clean/pprr_post_2020_11_06.csv"))
    return post[post.agency == "lafayette parish so"]


def fuse_events(cprr20, cprr14, cprr08, post):
    builder = events.Builder()
    builder.extract_events(
        cprr20,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "allegation_uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr14,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr08,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "parse_date": True,
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
    cprr20 = pd.read_csv(data_file_path("clean/cprr_lafayette_so_2015_2020.csv"))
    cprr14 = pd.read_csv(data_file_path("clean/cprr_lafayette_so_2009_2014.csv"))
    cprr08 = pd.read_csv(data_file_path("clean/cprr_lafayette_so_2006_2008.csv"))
    post = prepare_post()
    complaints = rearrange_allegation_columns(pd.concat([cprr20, cprr14, cprr08]))
    event = fuse_events(cprr20, cprr14, cprr08, post)
    personnel_df = fuse_personnel(cprr20, cprr14, cprr08, post)
    personnel_df.to_csv(data_file_path("fuse/per_lafayette_so.csv"), index=False)
    event.to_csv(data_file_path("fuse/event_lafayette_so.csv"), index=False)
    complaints.to_csv(data_file_path("fuse/com_lafayette_so.csv"), index=False)
