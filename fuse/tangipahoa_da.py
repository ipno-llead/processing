import sys

sys.path.append("../")
from lib.path import data_file_path
from lib.columns import rearrange_allegation_columns
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
    brady = pd.read_csv(data_file_path("match/cprr_tangipahoa_da_2021.csv"))
    agency = brady.agency[0]
    post = load_for_agency("clean/pprr_post_2020_11_06.csv", agency)
    per = fuse_personnel(brady, post)
    complaints = rearrange_allegation_columns(brady)
    event = fuse_events(post)
    complaints.to_csv(data_file_path("fuse/com_tangipahoa_da.csv"), index=False)
    per.to_csv(data_file_path("fuse/per_tangipahoa_da.csv"), index=False)
    event.to_csv(data_file_path("fuse/event_tangipahoa_da.csv"), index=False)
    brady.to_csv(data_file_path("fuse/brady_tangipahoa_da.csv"), index=False)
