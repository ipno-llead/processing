import sys

import pandas as pd

from lib.path import data_file_path, ensure_data_dir
from lib.columns import rearrange_personnel_columns, rearrange_event_columns
from lib import events

sys.path.append("../")


def fuse_events(pprr):
    builder = events.Builder()
    builder.extract_events(
        pprr,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": [
                    "uid",
                    "agency",
                    "rank_desc",
                    "employment_status",
                    "salary",
                    "salary_freq",
                    "salary",
                    "salary_freq",
                ],
            },
            events.OFFICER_RANK: {
                "prefix": "rank",
                "keep": [
                    "uid",
                    "agency",
                    "rank_desc",
                    "salary",
                    "salary_freq",
                    "salary",
                    "salary_freq",
                ],
            },
        },
        ["uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    pprr = pd.read_csv(data_file_path("clean/pprr_vivian_pd_2021.csv"))
    post_event = pd.read_csv(data_file_path("match/post_event_vivian_pd_2020.csv"))
    events_df = rearrange_event_columns(pd.concat([post_event, fuse_events(pprr)]))
    ensure_data_dir("fuse")
    rearrange_personnel_columns(pprr).to_csv(
        data_file_path("fuse/per_vivian_pd.csv"), index=False
    )
    events_df.to_csv(data_file_path("fuse/event_vivian_pd.csv"), index=False)
