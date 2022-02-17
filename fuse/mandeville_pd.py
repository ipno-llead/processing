from lib import events
from lib.columns import rearrange_allegation_columns, rearrange_event_columns
import dirk
from lib.personnel import fuse_personnel
import pandas as pd


def fuse_events(cprr, pprr):
    builder = events.Builder()
    builder.extract_events(
        cprr,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "allegation_uid", "agency"],
            },
            events.COMPLAINT_INCIDENT: {
                "prefix": "occur",
                "keep": ["uid", "allegation_uid", "agency"],
            },
            events.INVESTIGATION_COMPLETE: {
                "prefix": "investigation_complete",
                "keep": ["uid", "allegation_uid", "agency"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        pprr,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": [
                    "uid",
                    "badge_no",
                    "rank_desc",
                    "salary",
                    "salary_freq",
                    "agency",
                ],
            },
            events.OFFICER_LEFT: {
                "prefix": "term",
                "keep": [
                    "uid",
                    "badge_no",
                    "rank_desc",
                    "salary",
                    "salary_freq",
                    "agency",
                ],
            },
        },
        ["uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    pprr = pd.read_csv(dirk.data("clean/pprr_mandeville_csd_2020.csv"))
    post_event = pd.read_csv(dirk.data("match/post_event_mandeville_pd_2019.csv"))
    cprr = pd.read_csv(dirk.data("match/cprr_mandeville_pd_2019.csv"))
    events_df = fuse_events(cprr, pprr)
    events_df = rearrange_event_columns(pd.concat([post_event, events_df]))
    per_df = fuse_personnel(pprr, cprr)
    com_df = rearrange_allegation_columns(cprr)
    events_df.to_csv(dirk.data("fuse/event_mandeville_pd.csv"), index=False)
    com_df.to_csv(dirk.data("fuse/com_mandeville_pd.csv"), index=False)
    per_df.to_csv(dirk.data("fuse/per_mandeville_pd.csv"), index=False)
