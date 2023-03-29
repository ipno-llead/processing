import pandas as pd
import deba
from lib.columns import rearrange_personnel_columns, rearrange_event_columns
from lib.personnel import fuse_personnel
from lib.post import load_for_agency


if __name__ == "__main__":
    pprr = pd.read_csv(deba.data("match/pprr_jefferson_so_2020.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    post_event = pd.read_csv(deba.data("match/post_event_jefferson_so_2020.csv"))
    events_df = rearrange_event_columns(post_event)
    per_df = rearrange_personnel_columns(pprr)
    per_df = fuse_personnel(per_df, post)
    per_df.to_csv(deba.data("fuse_agency/per_jefferson_so.csv"), index=False)
    events_df.to_csv(deba.data("fuse_agency/event_jefferson_so.csv"), index=False)
    post.to_csv(deba.data("fuse_agency/post_jefferson_so.csv"), index=False)
