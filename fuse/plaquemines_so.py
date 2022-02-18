import bolo
from lib.columns import rearrange_allegation_columns, rearrange_event_columns
from lib.personnel import fuse_personnel
from lib import events
import pandas as pd


def fuse_events(cprr19, cprr20, pprr):
    builder = events.Builder()
    builder.extract_events(
        cprr19,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "allegation_uid"],
            }
        },
        ["uid", "allegation_uid"],
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
        pprr,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": ["uid", "agency", "department_desc", "sub_department_desc"],
            }
        },
        ["uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr19 = pd.read_csv(bolo.data("clean/cprr_plaquemines_so_2019.csv"))
    cprr20 = pd.read_csv(bolo.data("clean/cprr_plaquemines_so_2016_2020.csv"))
    pprr = pd.read_csv(bolo.data("clean/pprr_plaquemines_so_2018.csv"))
    post_event = pd.read_csv(bolo.data("match/event_plaquemines_so_2018.csv"))
    events_df = fuse_events(cprr19, cprr20, pprr)
    fuse_personnel(pprr, cprr19, cprr20).to_csv(
        bolo.data("fuse/per_plaquemines_so.csv"), index=False
    )
    com = rearrange_allegation_columns(pd.concat([cprr19, cprr20]))
    com.to_csv(bolo.data("fuse/com_plaquemines_so.csv"), index=False)
    rearrange_event_columns(pd.concat([events_df, post_event])).to_csv(
        bolo.data("fuse/event_plaquemines_so.csv"), index=False
    )
