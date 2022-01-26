import sys

sys.path.append("../")
from lib.path import data_file_path
from lib.columns import rearrange_allegation_columns, rearrange_brady_list_columns
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
    brady = pd.read_csv(data_file_path("match/brady_baton_rouge_da_2021.csv"))
    agency = brady.agency[0]
    post = load_for_agency("clean/pprr_post_2020_11_06.csv", agency)
    per_df = fuse_personnel(brady)
    brady_df = rearrange_brady_list_columns(brady)
    event_df = fuse_events(post)
    per_df.to_csv(data_file_path("fuse/per_baton_rouge_da.csv"), index=False)
    event_df.to_csv(data_file_path("fuse/event_baton_rouge_da.csv"), index=False)
    brady_df.to_csv(data_file_path("fuse/brady_baton_rouge_da.csv"), index=False)
