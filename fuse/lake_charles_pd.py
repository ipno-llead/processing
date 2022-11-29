import pandas as pd
import deba
from lib import events
from lib.columns import (
    rearrange_allegation_columns,
    rearrange_event_columns,
    rearrange_citizen_columns,
)
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
    cprr20 = pd.read_csv(deba.data("match/cprr_lake_charles_pd_2020.csv"))
    cprr19 = pd.read_csv(deba.data("match/cprr_lake_charles_pd_2014_2019.csv"))
    pprr = pd.read_csv(deba.data("clean/pprr_lake_charles_pd_2017_2021.csv"))
    citizen_df = pd.read_csv(deba.data("clean/cprr_cit_lake_charles_pd_2014_2019.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    post_event = pd.read_csv(deba.data("match/post_event_lake_charles_2020_11_06.csv"))
    per_df = fuse_personnel(cprr20, cprr19, pprr, post)
    com_df = rearrange_allegation_columns(pd.concat([cprr20, cprr19], axis=0))
    event_df = rearrange_event_columns(
        pd.concat([fuse_events(cprr20, cprr19), post_event])
    )
    citizen_df = rearrange_citizen_columns(citizen_df)
    event_df.to_csv(deba.data("fuse/event_lake_charles_pd.csv"), index=False)
    com_df.to_csv(deba.data("fuse/com_lake_charles_pd.csv"), index=False)
    per_df.to_csv(deba.data("fuse/per_lake_charles_pd.csv"), index=False)
    post.to_csv(deba.data("fuse/post_lake_charles_pd.csv"), index=False)
    citizen_df.to_csv(deba.data("fuse/cit_lake_charles_pd.csv"), index=False)
