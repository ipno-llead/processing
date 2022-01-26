import sys

sys.path.append("../")
from lib.path import data_file_path
from lib.columns import rearrange_brady_list_columns
import pandas as pd
from lib import events


def fuse_events(brady):
    builder = events.Builder()
    builder.extract_events(
        brady,
        {
            events.INVESTIGATION_COMPLETE: {
                "prefix": "investigation_complete",
                "keep": ["agency", "uid", "allegation_uid", "brady_uid"],
            },
        },
        ["uid", "allegation_uid", "brady_uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    brady = pd.read_csv(data_file_path("match/brady_ouachita_da_2021.csv"))
    event_df = fuse_events(brady)
    brady_df = rearrange_brady_list_columns(brady)
    event_df.to_csv(data_file_path("fuse/event_ouachita_da.csv"), index=False)
    brady_df.to_csv(data_file_path("fuse/brady_ouachita_da.csv"), index=False)
