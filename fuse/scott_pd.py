import dirk
from lib.columns import rearrange_allegation_columns, rearrange_event_columns
from lib.personnel import fuse_personnel
from lib import events
import pandas as pd


def fuse_events(pprr, cprr20, cprr14):
    builder = events.Builder()
    pprr.loc[:, "agency"] = "Scott PD"
    builder.extract_events(
        pprr,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": [
                    "uid",
                    "badge_no",
                    "agency",
                    "rank_desc",
                    "salary",
                    "salary_freq",
                ],
            },
            events.OFFICER_LEFT: {
                "prefix": "term",
                "keep": [
                    "uid",
                    "badge_no",
                    "agency",
                    "rank_desc",
                    "salary",
                    "salary_freq",
                ],
            },
        },
        ["uid"],
    )
    builder.extract_events(
        cprr20,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "allegation_uid"],
            }
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr14,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "allegation_uid"],
            }
        },
        ["uid", "allegation_uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr20 = pd.read_csv(dirk.data("match/cprr_scott_pd_2020.csv"))
    cprr14 = pd.read_csv(dirk.data("match/cprr_scott_pd_2009_2014.csv"))
    pprr = pd.read_csv(dirk.data("clean/pprr_scott_pd_2021.csv"))
    post_event = pd.read_csv(dirk.data("match/post_event_scott_pd_2021.csv"))
    personnels = fuse_personnel(pprr, cprr20, cprr14)
    complaints = rearrange_allegation_columns(pd.concat([cprr20, cprr14]))
    events_df = fuse_events(pprr, cprr20, cprr14)
    events_df = rearrange_event_columns(pd.concat([post_event, events_df]))
    personnels.to_csv(dirk.data("fuse/per_scott_pd.csv"), index=False)
    events_df.to_csv(dirk.data("fuse/event_scott_pd.csv"), index=False)
    complaints.to_csv(dirk.data("fuse/com_scott_pd.csv"), index=False)
