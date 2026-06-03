import pandas as pd
import deba
from lib.columns import rearrange_event_columns, rearrange_use_of_force
from lib.personnel import fuse_personnel
from lib.post import load_for_agency
from lib import events


def fuse_events(uof):
    builder = events.Builder()
    builder.extract_events(
        uof,
        {
            events.UOF_INCIDENT: {
                "prefix": "occurred",
                "keep": ["uid", "uof_uid", "agency"],
            },
        },
        ["uid", "uof_uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    uof = pd.read_csv(deba.data("match/uof_monroe_pd_2024_2026.csv"))
    agency = uof.agency[0]
    post = load_for_agency(agency)
    per_df = fuse_personnel(uof, post)
    event_df = rearrange_event_columns(fuse_events(uof))
    uof_df = rearrange_use_of_force(uof)
    per_df.to_csv(deba.data("fuse_agency/per_monroe_pd.csv"), index=False)
    event_df.to_csv(deba.data("fuse_agency/event_monroe_pd.csv"), index=False)
    uof_df.to_csv(deba.data("fuse_agency/uof_monroe_pd.csv"), index=False)
    post.to_csv(deba.data("fuse_agency/post_monroe_pd.csv"), index=False)
