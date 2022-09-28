import deba
from lib.columns import rearrange_allegation_columns, rearrange_event_columns
from lib.personnel import fuse_personnel
from lib import events
import pandas as pd
from lib.post import load_for_agency


def fuse_events(pprr, cprr20, cprr14):
    builder = events.Builder()
    pprr.loc[:, "agency"] = "scott-pd"
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
    cprr20 = pd.read_csv(deba.data("match/cprr_scott_pd_2020.csv"))
    cprr14 = pd.read_csv(deba.data("match/cprr_scott_pd_2009_2014.csv"))
    pprr = pd.read_csv(deba.data("clean/pprr_scott_pd_2021.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    post_event = pd.read_csv(deba.data("match/post_event_scott_pd_2021.csv"))
    personnels = fuse_personnel(pprr, cprr20, cprr14, post)
    complaints = rearrange_allegation_columns(pd.concat([cprr20, cprr14]))
    events_df = fuse_events(pprr, cprr20, cprr14)
    events_df = rearrange_event_columns(pd.concat([post_event, events_df]))
    personnels.to_csv(deba.data("fuse/per_scott_pd.csv"), index=False)
    events_df.to_csv(deba.data("fuse/event_scott_pd.csv"), index=False)
    complaints.to_csv(deba.data("fuse/com_scott_pd.csv"), index=False)
