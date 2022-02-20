import pandas as pd
import bolo
from lib import events
from lib.columns import rearrange_allegation_columns, rearrange_event_columns
from lib.personnel import fuse_personnel
from lib.post import load_for_agency


def fuse_events(cprr20, cprr19):
    builder = events.Builder()
    builder.extract_events(
        cprr20,
        {
            events.INVESTIGATION_START: {
                "prefix": "investigation_start",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr19,
        {
            events.INVESTIGATION_START: {
                "prefix": "investigation_start",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr20 = pd.read_csv(bolo.data("match/cprr_lake_charles_pd_2020.csv"))
    cprr19 = pd.read_csv(bolo.data("match/cprr_lake_charles_pd_2014_2019.csv"))
    pprr = pd.read_csv(bolo.data("clean/pprr_lake_charles_pd_2017_2021.csv"))
    post_event = pd.read_csv(bolo.data("match/post_event_lake_charles_2020_11_06.csv"))
    per_df = fuse_personnel(cprr20, cprr19, pprr)
    com_df = rearrange_allegation_columns(pd.concat([cprr20, cprr19]))
    event_df = rearrange_event_columns(
        pd.concat([fuse_events(cprr20, cprr19), post_event])
    )
    event_df.to_csv(bolo.data("fuse/event_lake_charles_pd.csv"), index=False)
    com_df.to_csv(bolo.data("fuse/com_lake_charles_pd.csv"), index=False)
    per_df.to_csv(bolo.data("fuse/per_lake_charles_pd.csv"), index=False)
