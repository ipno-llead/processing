import pandas as pd
import deba
from lib.columns import rearrange_event_columns, rearrange_personnel_columns
from lib import events


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
                    "employee_id",
                    "rank_desc",
                    "salary",
                    "salary_freq",
                ],
            }
        },
        ["uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    pprr = pd.read_csv(deba.data("match/pprr_caddo_parish_so_2020.csv"))
    post_event = pd.read_csv(deba.data("match/post_event_caddo_parish_so.csv"))
    cprr_post_event = pd.read_csv(
        deba.data("match/cprr_post_event_caddo_parish_so.csv")
    )
    event_df = fuse_events(pprr)
    event_df = rearrange_event_columns(
        pd.concat([post_event, event_df, cprr_post_event])
    )
    rearrange_personnel_columns(pprr).to_csv(
        deba.data("fuse/per_caddo_parish_so.csv"), index=False
    )
    event_df.to_csv(deba.data("fuse/event_caddo_parish_so.csv"), index=False)
