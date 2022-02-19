import pandas as pd

import bolo
from lib.columns import (
    rearrange_allegation_columns,
    rearrange_personnel_columns,
    rearrange_event_columns,
)
from lib import events


def fuse_events(pprr, cprr):
    builder = events.Builder()
    builder.extract_events(
        pprr,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": [
                    "uid",
                    "agency",
                    "employee_id",
                    "department_desc",
                    "salary",
                    "salary_freq",
                ],
            },
            events.OFFICER_LEFT: {
                "prefix": "termination",
                "parse_date": True,
                "keep": [
                    "uid",
                    "agency",
                    "employee_id",
                    "department_desc",
                    "salary",
                    "salary_freq",
                ],
            },
        },
        ["uid"],
    )
    builder.extract_events(
        cprr,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "parse_date": True,
                "keep": ["uid", "allegation_uid", "agency"],
            }
        },
        ["uid", "allegation_uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    pprr = pd.read_csv(bolo.data("clean/pprr_ponchatoula_pd_2010_2020.csv"))
    cprr = pd.read_csv(bolo.data("match/cprr_ponchatoula_pd_2010_2020.csv"))
    post_event = pd.read_csv(bolo.data("match/post_event_ponchatoula_pd_2020.csv"))
    cprr_post_events = pd.read_csv(
        bolo.data("match/cprr_post_events_ponchatoula_pd_2020.csv")
    )
    event_df = rearrange_event_columns(
        pd.concat([fuse_events(pprr, cprr), post_event, cprr_post_events])
    )
    rearrange_personnel_columns(pprr).to_csv(
        bolo.data("fuse/per_ponchatoula_pd.csv"), index=False
    )
    rearrange_allegation_columns(cprr).to_csv(
        bolo.data("fuse/com_ponchatoula_pd.csv"), index=False
    )
    event_df.to_csv(bolo.data("fuse/event_ponchatoula_pd.csv"), index=False)
