import bolo
from lib.columns import rearrange_allegation_columns, rearrange_event_columns
from lib.personnel import fuse_personnel
from lib.post import load_for_agency
import pandas as pd
from lib import events


def fuse_events(pprr):
    builder = events.Builder()
    builder.extract_events(
        pprr,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": ["uid", "agency", "rank_desc"],
            },
            events.OFFICER_LEFT: {
                "prefix": "resignation",
                "keep": ["uid", "agency", "rank_desc"],
            },
        },
        ["uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr = pd.read_csv(bolo.data("match/cprr_baker_pd_2018_2020.csv"))
    pprr = pd.read_csv(bolo.data("clean/pprr_baker_pd_2010_2020.csv"))
    post_event = pd.read_csv(bolo.data("match/post_event_baker_pd_2020_11_06.csv"))
    agency = cprr.agency[0]
    post = load_for_agency(agency)
    per_df = fuse_personnel(cprr, pprr)
    com_df = rearrange_allegation_columns(cprr)
    event_df = fuse_events(pprr)
    event_df = rearrange_event_columns(pd.concat([post_event, event_df]))
    com_df.to_csv(bolo.data("fuse/com_baker_pd.csv"), index=False)
    per_df.to_csv(bolo.data("fuse/per_baker_pd.csv"), index=False)
    event_df.to_csv(bolo.data("fuse/event_baker_pd.csv"), index=False)
