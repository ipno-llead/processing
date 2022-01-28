import sys

import pandas as pd

from lib.path import data_file_path
from lib.columns import rearrange_event_columns
from lib.personnel import fuse_personnel
from lib import events

sys.path.append("../")


def fuse_events(pprr):
    builder = events.Builder()
    builder.extract_events(
        pprr,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": ["uid", "agency", "rank_desc"],
            },
            events.OFFICER_LEFT: {
                "prefix": "left",
                "keep": ["uid", "agency", "rank_desc", "left_reason"],
            },
        },
        ["uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    pprr = pd.read_csv(data_file_path("clean/pprr_rayne_pd_2010_2020.csv"))
    post_event = pd.read_csv(data_file_path("match/post_event_rayne_pd_2020_11_06.csv"))
    events_df = rearrange_event_columns(pd.concat([post_event, fuse_events(pprr)]))
    fuse_personnel(pprr).to_csv(data_file_path("fuse/per_rayne_pd.csv"), index=False)
    events_df.to_csv(data_file_path("fuse/event_rayne_pd.csv"), index=False)
