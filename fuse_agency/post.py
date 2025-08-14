import pandas as pd
import deba
from lib.personnel import fuse_personnel
from lib.columns import (
    rearrange_event_columns,
    rearrange_allegation_columns,
    rearrange_personnel_columns,
)
from lib import events


def fuse_events(post, cprr):
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
    builder.extract_events(
        cprr,
        {
            events.OFFICER_POST_DECERTIFICATION: {
                "prefix": "decertification",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    post = pd.read_csv(deba.data("clean/pprr_post_4_26_2023.csv"))
    cprr = pd.read_csv(deba.data("match/cprr_post_decertifications_2016_2023.csv"))
    event_df = fuse_events(post, cprr)
    event_df = rearrange_event_columns(event_df)
    per_df = fuse_personnel(post, cprr)
    per_df = rearrange_personnel_columns(per_df)
    allegation_df = rearrange_allegation_columns(cprr)
    allegation_df.to_csv(deba.data("fuse_agency/com_post.csv"), index=False)
    event_df.to_csv(deba.data("fuse_agency/event_post.csv"), index=False)
    per_df.to_csv(deba.data("fuse_agency/per_post.csv"), index=False)
