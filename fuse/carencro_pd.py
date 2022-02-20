import pandas as pd

import bolo
from lib.columns import rearrange_personnel_columns, rearrange_event_columns


if __name__ == "__main__":
    pprr = pd.read_csv(bolo.data("clean/pprr_carencro_pd_2021.csv"))
    post_event = pd.read_csv(bolo.data("match/post_event_carencro_pd_2020_11_06.csv"))
    events_df = rearrange_event_columns(post_event)
    per_df = rearrange_personnel_columns(pprr)
    per_df.to_csv(bolo.data("fuse/per_carencro_pd.csv"), index=False)
    events_df.to_csv(bolo.data("fuse/event_carencro_pd.csv"), index=False)
