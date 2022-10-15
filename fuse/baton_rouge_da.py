import pandas as pd
import deba
from lib.columns import (
    rearrange_brady_columns,
)

from lib import events


def fuse_events(brady18, brady21):
    builder = events.Builder()
    builder.extract_events(
        brady18,
        {
            events.BRADY_LIST: {
                "prefix": "brady_list",
                "parse_date": True,
                "keep": [
                    "uid",
                    "brady_uid",
                    "agency",
                    "source_agency",
                    "allegation_desc",
                ],
            },
        },
        ["uid"],
    )
    builder.extract_events(
        brady21,
        {
            events.BRADY_LIST: {
                "prefix": "brady_list",
                "parse_date": True,
                "keep": [
                    "uid",
                    "brady_uid",
                    "agency",
                    "source_agency",
                    "action",
                    "allegation",
                ],
            },
        },
        ["uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    brady21 = pd.read_csv(deba.data("match/brady_baton_rouge_da_2021.csv"))
    brady18 = pd.read_csv(deba.data("match/brady_baton_rouge_da_2018.csv"))
    brady_df = rearrange_brady_columns(pd.concat([brady21, brady18]))
    event_df = fuse_events(brady21, brady18)
    brady_df.to_csv(deba.data("fuse/brady_baton_rouge_da.csv"), index=False)
    event_df.to_csv(deba.data("fuse/event_baton_rouge_da.csv"), index=False)
