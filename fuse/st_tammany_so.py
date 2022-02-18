import bolo
from lib.columns import rearrange_allegation_columns, rearrange_event_columns
from lib.personnel import fuse_personnel
from lib import events
import pandas as pd


def fuse_events(pprr, cprr):
    builder = events.Builder()
    builder.extract_events(
        pprr,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": ["uid", "agency", "rank_code", "rank_desc", "pay_group"],
            },
            events.OFFICER_LEFT: {
                "prefix": "term",
                "keep": ["uid", "agency", "rank_code", "rank_desc", "pay_group"],
            },
        },
        ["uid"],
    )
    builder.extract_events(
        cprr,
        {
            events.COMPLAINT_INCIDENT: {
                "prefix": "occur",
                "keep": ["uid", "agency", "allegation_uid"],
            }
        },
        ["uid", "allegation_uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr = pd.read_csv(bolo.data("match/cprr_st_tammany_so_2011_2021.csv"))
    pprr = pd.read_csv(bolo.data("clean/pprr_st_tammany_so_2020.csv"))
    post_event = pd.read_csv(bolo.data("match/post_event_st_tammany_so_2020.csv"))
    personnels = fuse_personnel(pprr, cprr)
    complaints = rearrange_allegation_columns(cprr)
    events_df = fuse_events(pprr, cprr)
    events_df = rearrange_event_columns(pd.concat([post_event, events_df]))
    personnels.to_csv(bolo.data("fuse/per_st_tammany_so.csv"), index=False)
    events_df.to_csv(bolo.data("fuse/event_st_tammany_so.csv"), index=False)
    complaints.to_csv(bolo.data("fuse/com_st_tammany_so.csv"), index=False)
