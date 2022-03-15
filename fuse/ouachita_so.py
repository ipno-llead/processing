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
            events.INVESTIGATION_COMPLETE: {
                "prefix": "investigation_complete",
                "keep": ["brady_uid", "agency"],
            },
        },
        ["uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    brady = pd.read_csv(deba.data("match/brady_ouachita_da_2021.csv"))
    brady = brady.loc[brady.agency == "Ouachita SO"]
    per_df = fuse_personnel(brady)
    event_df = fuse_events(brady)
    brady_df = rearrange_brady_columns(brady)
    event_df.to_csv(deba.data("fuse/event_ouachita_so.csv"), index=False)
    per_df.to_csv(deba.data("fuse/brady_ouachita_so.csv"), index=False)
    brady_df.to_csv(deba.data("fuse/brady_ouachita_so.csv"), index=False)
