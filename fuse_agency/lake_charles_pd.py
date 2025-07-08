import pandas as pd
import deba
from lib import events
from lib.columns import (
    rearrange_allegation_columns,
    rearrange_event_columns,
    rearrange_citizen_columns,
    rearrange_use_of_force
)
from lib.personnel import fuse_personnel
from lib.post import load_for_agency


def fuse_events(cprr, uof):
    builder = events.Builder()
    builder.extract_events(
        cprr,
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
        uof,
        {
            events.UOF_INCIDENT: {
                "prefix": "incident",
                "keep": ["uid", "agency", "use_of_force_description"],
            },
        },
        ["uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr = pd.read_csv(deba.data("match/cprr_lake_charles_pd_2014_2023.csv"))
    pprr = pd.read_csv(deba.data("clean/pprr_lake_charles_pd_2017_2021.csv"))
    uof = pd.read_csv(deba.data("match/uof_lake_charles_pd_2020.csv"))
    citizen_df = pd.read_csv(deba.data("clean/cprr_cit_lake_charles_pd_2014_2019.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    post_event = pd.read_csv(deba.data("match/post_event_lake_charles_2020_11_06.csv"))
    per_df = fuse_personnel(cprr, pprr, post)
    com_df = rearrange_allegation_columns(cprr)
    event_df = rearrange_event_columns(
        pd.concat([fuse_events(cprr, uof), post_event])
    )
    citizen_df = rearrange_citizen_columns(citizen_df)
    uof = rearrange_use_of_force(uof)
    event_df.to_csv(deba.data("fuse_agency/event_lake_charles_pd.csv"), index=False)
    com_df.to_csv(deba.data("fuse_agency/com_lake_charles_pd.csv"), index=False)
    per_df.to_csv(deba.data("fuse_agency/per_lake_charles_pd.csv"), index=False)
    post.to_csv(deba.data("fuse_agency/post_lake_charles_pd.csv"), index=False)
    citizen_df.to_csv(deba.data("fuse_agency/cit_lake_charles_pd.csv"), index=False)
    uof.to_csv(deba.data("fuse_agency/uof_lake_charles_pd.csv"), index=False)
