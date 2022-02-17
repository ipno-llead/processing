import pandas as pd

import dirk
from lib.columns import (
    rearrange_allegation_columns,
    rearrange_personnel_columns,
    rearrange_event_columns,
)
from lib import events


def fuse_events(cprr, pprr):
    builder = events.Builder()
    builder.extract_events(
        pprr,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "parse_date": True,
                "keep": [
                    "agency",
                    "uid",
                    "rank_desc",
                    "badge_no",
                    "salary",
                    "salary_freq",
                ],
            }
        },
        ["uid"],
    )
    builder.extract_events(
        cprr,
        {
            events.COMPLAINT_INCIDENT: {
                "prefix": "occur",
                "keep": ["uid", "agency", "allegation_uid"],
            },
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "allegation_uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    pprr = pd.read_csv(dirk.data("clean/pprr_west_monroe_pd_2015_2020.csv"))
    cprr = pd.read_csv(dirk.data("clean/cprr_west_monroe_pd_2020.csv"))
    post_event = pd.read_csv(
        dirk.data("match/post_event_west_monroe_pd_2020_11_06.csv")
    )
    per_df = rearrange_personnel_columns(pprr)
    com_df = rearrange_allegation_columns(cprr)
    events_df = rearrange_event_columns(
        pd.concat([fuse_events(cprr, pprr), post_event])
    )
    per_df.to_csv(dirk.data("fuse/per_west_monroe_pd.csv"), index=False)
    com_df.to_csv(dirk.data("fuse/com_west_monroe_pd.csv"), index=False)
    events_df.to_csv(dirk.data("fuse/event_west_monroe_pd.csv"), index=False)
