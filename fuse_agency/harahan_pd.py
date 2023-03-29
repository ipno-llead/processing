import deba
from lib.columns import rearrange_event_columns, rearrange_personnel_columns
from lib.personnel import fuse_personnel
from lib.post import load_for_agency
import pandas as pd
from lib import events


def fuse_events(post):
    builder = events.Builder()
    builder.extract_events(
        post,
        {
            events.OFFICER_LEVEL_1_CERT: {
                "prefix": "level_1_cert",
                "parse_date": "%Y-%m-%d",
                "keep": ["uid", "agency"],
            },
            events.OFFICER_PC_12_QUALIFICATION: {
                "prefix": "last_pc_12_qualification",
                "parse_date": "%Y-%m-%d",
                "keep": ["uid", "agency"],
            },
            events.OFFICER_HIRE: {"prefix": "hire", "keep": ["uid", "agency"]},
        },
        ["uid"],
    )

    return builder.to_frame()

if __name__ == "__main__":
    pprr = pd.read_csv(deba.data("match/pprr_harahan_pd_2020.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    events_df = fuse_events(post)
    events_df = rearrange_event_columns(events_df)
    per_df = rearrange_personnel_columns(pprr)
    per_df = fuse_personnel(per_df, post)
    per_df.to_csv(deba.data("fuse_agency/per_harahan_pd.csv"), index=False)
    events_df.to_csv(deba.data("fuse_agency/event_harahan_pd.csv"), index=False)
    post.to_csv(deba.data("fuse_agency/post_harahan_pd.csv"), index=False)
