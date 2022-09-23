import pandas as pd
import deba
from lib.columns import (
    rearrange_personnel_columns,
    rearrange_event_columns,
    rearrange_post_officer_history_columns,
)
from lib import events


def fuse_events(post):
    builder = events.Builder()
    builder.extract_events(
        post,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "parse_date": True,
                "keep": [
                    "uid",
                    "agency",
                ],
            },
            events.OFFICER_LEFT: {
                "prefix": "left",
                "parse_date": True,
                "keep": [
                    "uid",
                    "left_reason",
                    "agency",
                ],
            },
        },
        ["uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    post = pd.read_csv(deba.data("match/post_officer_history.csv"))

    events_pre_post = pd.read_csv(deba.data("fuse/event_pre_post.csv"))
    per_pre_post = pd.read_csv(deba.data("fuse/personnel_pre_post.csv"))

    post_events = fuse_events(post)
    event_df = pd.concat([post_events, events_pre_post], axis=0).drop_duplicates(
        subset=["event_uid"], keep="last"
    )
    event_df = rearrange_event_columns(event_df)
    per_df = rearrange_personnel_columns(pd.concat([per_pre_post, post]))
    post = rearrange_post_officer_history_columns(post)

    per_df.to_csv(deba.data("fuse/personnel.csv"), index=False)
    event_df.to_csv(deba.data("fuse/event.csv"), index=False)
    post.to_csv(deba.data("fuse/post_officer_history.csv"), index=False)
