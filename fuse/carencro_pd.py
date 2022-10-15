import pandas as pd

import deba
from lib.columns import rearrange_personnel_columns, rearrange_event_columns
from lib.personnel import fuse_personnel
from lib.post import load_for_agency


if __name__ == "__main__":
    pprr = pd.read_csv(deba.data("clean/pprr_carencro_pd_2021.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    post_event = pd.read_csv(deba.data("match/post_event_carencro_pd_2020_11_06.csv"))
    events_df = rearrange_event_columns(post_event)
    per_df = rearrange_personnel_columns(pprr)
    per_df = fuse_personnel(per_df, post)
    per_df.to_csv(deba.data("fuse/per_carencro_pd.csv"), index=False)
    events_df.to_csv(deba.data("fuse/event_carencro_pd.csv"), index=False)
    post.to_csv(deba.data("fuse/post_carencro_pd.csv"), index=False)
