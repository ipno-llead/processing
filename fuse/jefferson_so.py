import pandas as pd
import deba
from lib.columns import rearrange_personnel_columns, rearrange_event_columns


if __name__ == "__main__":
    pprr = pd.read_csv(deba.data("match/pprr_jefferson_so_2020.csv"))
    post_event = pd.read_csv(deba.data("match/post_event_jefferson_so_2020.csv"))
    events_df = rearrange_event_columns(post_event)
    per_df = rearrange_personnel_columns(pprr)
    per_df.to_csv(deba.data("fuse/per_jefferson_so.csv"), index=False)
    events_df.to_csv(deba.data("fuse/event_jefferson_so.csv"), index=False)
