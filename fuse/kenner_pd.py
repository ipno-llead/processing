import sys

import pandas as pd
from lib.path import data_file_path
from lib.columns import rearrange_event_columns, rearrange_use_of_force
from lib.personnel import fuse_personnel
from lib import events

sys.path.append("../")


def fuse_events(pprr, uof):
    builder = events.Builder()
    builder.extract_events(
        pprr,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": [
                    "uid",
                    "agency",
                    "department_desc",
                    "rank_desc",
                    "sworn",
                    "officer_inactive",
                    "employee_class",
                    "employment_status",
                ],
            },
            events.OFFICER_LEFT: {
                "prefix": "left",
                "keep": [
                    "uid",
                    "agency",
                    "department_desc",
                    "rank_desc",
                    "sworn",
                    "officer_inactive",
                    "employee_class",
                    "employment_status",
                ],
            },
        },
        ["uid"],
    )
    builder.extract_events(
        uof,
        {
            events.UOF_INCIDENT: {
                "prefix": "incident",
            },
        },
        ["uid", "uof_uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    post_event = pd.read_csv(data_file_path("match/post_event_kenner_pd_2020.csv"))
    pprr = pd.read_csv(data_file_path("clean/pprr_kenner_pd_2020.csv"))
    uof = pd.read_csv(data_file_path("clean/uof_kenner_pd_2005_2021.csv"))
    cprr_post_events = pd.read_csv(
        data_file_path("match/cprr_post_events_kenner_pd_2020.csv")
    )
    per = fuse_personnel(pprr, uof)
    uof_df = rearrange_use_of_force(uof)
    events_df = rearrange_event_columns(
        pd.concat([post_event, cprr_post_events, fuse_events(pprr, uof)])
    )
    per.to_csv(data_file_path("fuse/per_kenner_pd.csv"), index=False)
    events_df.to_csv(data_file_path("fuse/event_kenner_pd.csv"), index=False)
    uof.to_csv(data_file_path("fuse/uof_kenner_pd.csv"), index=False)
