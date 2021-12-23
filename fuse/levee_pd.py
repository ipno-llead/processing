from lib.columns import rearrange_allegation_columns
from lib.personnel import fuse_personnel
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
            events.COMPLAINT_INCIDENT: {
                "prefix": "occur",
                "parse_date": True,
                "keep": ["agency", "uid", "allegation_uid"],
            },
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "parse_date": True,
                "keep": ["agency", "uid", "allegation_uid"],
            },
            events.INVESTIGATION_COMPLETE: {
                "prefix": "investigation_complete",
                "parse_date": True,
                "keep": ["agency", "uid", "allegation_uid"],
            },
            events.INVESTIGATION_START: {
                "prefix": "investigation_start",
                "parse_date": True,
                "keep": ["agency", "uid", "allegation_uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        post,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": ["agency", "uid", "employment_status"],
            },
            events.OFFICER_LEVEL_1_CERT: {
                "prefix": "level_1_cert",
                "parse_date": "%Y-%m-%d",
                "keep": ["agency", "uid"],
            },
            events.OFFICER_PC_12_QUALIFICATION: {
                "prefix": "last_pc_12_qualification",
                "parse_date": "%Y-%m-%d",
                "keep": ["agency", "uid"],
            },
        },
        ["uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr = pd.read_csv(data_file_path("match/cprr_levee_pd.csv"))
    post = pd.read_csv(data_file_path("clean/pprr_post_2020_11_06.csv"))
    event_df = fuse_events(cprr, post)
    event_df.to_csv(data_file_path("fuse/event_levee_pd.csv"), index=False)
    complaint_df = rearrange_allegation_columns(cprr)
    complaint_df.to_csv(data_file_path("fuse/com_levee_pd.csv"), index=False)
    fuse_personnel(post, cprr).to_csv(data_file_path("fuse/per_levee_pd.csv"), index=False)
