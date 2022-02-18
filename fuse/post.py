import pandas as pd
import bolo
from lib.personnel import fuse_personnel
from lib.columns import rearrange_personnel_columns
from lib.columns import rearrange_event_columns
from lib import events


def fuse_events(post):
    builder = events.Builder()
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
    post = pd.read_csv(bolo.data("clean/pprr_post_2020_11_06.csv"))
    event_df = fuse_events(post)
    event_df = rearrange_event_columns(event_df)
    per_df = fuse_personnel(post)
    per_df = rearrange_personnel_columns(per_df)
    event_df.to_csv(bolo.data("fuse/events_post.csv"), index=False)
    per_df.to_csv(bolo.data("fuse/per_post.csv"), index=False)
