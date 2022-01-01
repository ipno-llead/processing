import sys

import pandas as pd

from lib.path import data_file_path
from lib.columns import rearrange_personnel_columns, rearrange_event_columns

sys.path.append("../")


if __name__ == "__main__":
    pprr = pd.read_csv(data_file_path("clean/pprr_carencro_pd_2021.csv"))
    post_event = pd.read_csv(data_file_path("match/post_event_carencro_pd_2020_11_06.csv"))
    events_df = rearrange_event_columns(post_event)
    per_df = rearrange_personnel_columns(pprr)
    per_df.to_csv(data_file_path("fuse/per_carencro_pd.csv"), index=False)
    events_df.to_csv(data_file_path("fuse/event_carencro_pd.csv"), index=False)
