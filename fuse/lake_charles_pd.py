import sys

sys.path.append("../")
import pandas as pd
from lib.path import data_file_path
from lib import events
from lib.columns import rearrange_allegation_columns
from lib.personnel import fuse_personnel


def fuse_events(cprr20, cprr19, post):
    builder = events.Builder()
    builder.extract_events(
        cprr20,
        {
            events.INVESTIGATION_START: {
                "prefix": "investigation_start",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr19,
        {
            events.INVESTIGATION_START: {
                "prefix": "investigation_start",
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
    cprr20 = pd.read_csv(data_file_path("match/cprr_lake_charles_pd_2020.csv"))
    cprr19 = pd.read_csv(data_file_path("match/cprr_lake_charles_pd_2014_2019.csv"))
    post = pd.read_csv(data_file_path("clean/pprr_post_2020_11_06.csv"))
    per = fuse_personnel(cprr20, cprr19, post)
    com = rearrange_allegation_columns(pd.concat([cprr20, cprr19]))
    event = fuse_events(cprr20, cprr19, post)
    event.to_csv(data_file_path("fuse/event_lake_charles_pd.csv"), index=False)
    com.to_csv(data_file_path("fuse/com_lake_charles_pd.csv"), index=False)
    per.to_csv(data_file_path("fuse/per_lake_charles_pd.csv"), index=False)
