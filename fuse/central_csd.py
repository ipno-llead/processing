import sys

sys.path.append("../")
import pandas as pd
from lib.path import data_file_path
from lib.events import rearrange_event_columns
from lib.personnel import fuse_personnel
from lib import events


def fuse_events(pprr):
    builder = events.Builder()
    builder.extract_events(
        pprr,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "parse_date": True,
                "keep": ["uid", "agency", "rank_desc", "salary", "salary_freq"],
            },
            events.OFFICER_LEFT: {
                "prefix": "termination",
                "parse_date": True,
                "keep": ["uid", "agency", "rank_desc", "salary", "salary_freq"],
            },
        },
        ["uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    pprr = pd.read_csv(data_file_path("clean/pprr_central_csd_2014_2019.csv"))
    post_events = pd.read_csv(data_file_path("match/post_events_central_csd_2020.csv"))
    per_df = fuse_personnel(pprr)
    events_df = fuse_events(pprr)
    events_df = rearrange_event_columns(pd.concat([events_df, post_events]))
    per_df.to_csv(data_file_path("fuse/per_central_csd.csv"), index=False)
    events_df.to_csv(data_file_path("fuse/event_central_csd.csv"), index=False)
