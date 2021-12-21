import sys

sys.path.append("../")
import pandas as pd
from lib.path import data_file_path
from lib.columns import rearrange_personnel_columns, rearrange_event_columns
from lib.personnel import fuse_personnel


if __name__ == "__main__":
    pprr = pd.read_csv(data_file_path("clean/pprr_jefferson_so_2020.csv"))
    post_event = pd.read_csv(data_file_path("match/post_event_jefferson_so_2020.csv"))
    personnel_df = fuse_personnel(pprr)
    events_df = rearrange_event_columns(post_event)
    rearrange_personnel_columns(pprr).to_csv(
        data_file_path("fuse/per_jefferson_so.csv"), index=False
    )
    events_df.to_csv(data_file_path("fuse/event_jefferson_so.csv"), index=False)
