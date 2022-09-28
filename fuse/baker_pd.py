import deba
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
    cprr20 = pd.read_csv(deba.data("match/cprr_baker_pd_2018_2020.csv"))
    cprr17 = pd.read_csv(deba.data("match/cprr_baker_pd_2014_2017.csv"))
    pprr = pd.read_csv(deba.data("clean/pprr_baker_pd_2010_2020.csv"))
    post_event = pd.read_csv(deba.data("match/post_event_baker_pd_2020_11_06.csv"))
    agency = cprr20.agency[0]
    post = load_for_agency(agency)
    per_df = fuse_personnel(cprr20, pprr, cprr17, post)
    com_df = rearrange_allegation_columns(pd.concat([cprr20, cprr17]))
    event_df = fuse_events(pprr)
    event_df = rearrange_event_columns(pd.concat([post_event, event_df]))
    com_df.to_csv(deba.data("fuse/com_baker_pd.csv"), index=False)
    per_df.to_csv(deba.data("fuse/per_baker_pd.csv"), index=False)
    event_df.to_csv(deba.data("fuse/event_baker_pd.csv"), index=False)
