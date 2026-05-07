import deba
import pandas as pd
from lib.columns import rearrange_event_columns, rearrange_use_of_force
from lib import events
from lib.personnel import fuse_personnel
from lib.post import load_for_agency

def fuse_events(pprr, uof):
    builder = events.Builder()
    builder.extract_events(
        pprr,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": ["uid", "agency", "rank_desc", "salary", "salary_freq"],
            },
            events.OFFICER_LEFT: {
                "prefix": "termination",
                "keep": ["uid", "agency", "rank_desc", "salary", "salary_freq"],
            },
        },
        ["uid"],
    )
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
    pprr = pd.read_csv(deba.data("clean/pprr_gonzales_pd_2010_2021.csv"))
    pprr_26 = pd.read_csv(deba.data("match/pprr_gonzales_pd_2026.csv"))
    uof = pd.read_csv(deba.data("match/uof_gonzales_pd_2023_2026.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    post_event = pd.read_csv(deba.data("match/post_event_gonzales_pd_2010_2021.csv"))
    post_event_26 = pd.read_csv(deba.data("match/post_event_gonzales_pd_2026.csv"))
    events_df = rearrange_event_columns(
        pd.concat([post_event, post_event_26, fuse_events(pprr, uof)])
    )
    per_df = fuse_personnel(pprr, pprr_26, uof, post)
    uof_df = rearrange_use_of_force(uof)
    per_df.to_csv(deba.data("fuse_agency/per_gonzales_pd.csv"), index=False)
    events_df.to_csv(deba.data("fuse_agency/event_gonzales_pd.csv"), index=False)
    uof_df.to_csv(deba.data("fuse_agency/uof_gonzales_pd.csv"), index=False)
    post.to_csv(deba.data("fuse_agency/post_gonzales_pd.csv"), index=False)
