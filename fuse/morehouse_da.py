import deba
from lib.columns import (
    rearrange_brady_columns,
)
from lib import events
from lib.personnel import fuse_personnel
import pandas as pd


def fuse_events(brady):
    builder = events.Builder()
    builder.extract_events(
        brady,
        {
            events.BRADY_LIST: {
                "prefix": "brady_list",
                "parse_date": True,
                "keep": [
                    "uid",
                    "brady_uid",
                    "agency",
                    "source_agency",
                    "allegation",
                    "action",
                    "initial_allegation",
                ],
            },
        },
        ["uid", "brady_uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    brady = pd.read_csv(deba.data("match/brady_morehouse_da_2022.csv"))
    event_df = fuse_events(brady)
    brady_df = rearrange_brady_columns(brady)
    event_df = event_df[~((event_df.uid.fillna("") == ""))]
    brady_df = brady_df[~((brady_df.uid.fillna("") == ""))]
    event_df.to_csv(deba.data("fuse/event_morehouse_da.csv"), index=False)
    brady_df.to_csv(deba.data("fuse/brady_morehouse_da.csv"), index=False)
