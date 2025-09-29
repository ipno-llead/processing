import pandas as pd
import deba
from lib.columns import (
    rearrange_event_columns,
    rearrange_use_of_force,
)
from lib.personnel import fuse_personnel
from lib import events
from lib.post import load_for_agency


def fuse_events(pprr, uof, uof25):
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
    builder.extract_events(
        uof25,
        {
            events.UOF_INCIDENT: {
                "prefix": "incident",
            },
        },
        ["uid", "uof_uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    post_event = pd.read_csv(deba.data("match/post_event_kenner_pd_2020.csv"))
    pprr = pd.read_csv(deba.data("clean/pprr_kenner_pd_2025.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    uof = pd.read_csv(deba.data("match/uof_kenner_pd_2005_2021.csv"))
    uof25 = pd.read_csv(deba.data("match/uof_kenner_pd_2021_2025.csv"))
    per = fuse_personnel(pprr, uof, post, uof25)
    uof_df = rearrange_use_of_force(pd.concat([uof, uof25]))
    events_df = rearrange_event_columns(
        pd.concat([post_event, fuse_events(pprr, uof, uof25)])
    )
    per.to_csv(deba.data("fuse_agency/per_kenner_pd.csv"), index=False)
    events_df.to_csv(deba.data("fuse_agency/event_kenner_pd.csv"), index=False)
    uof_df.to_csv(deba.data("fuse_agency/uof_kenner_pd.csv"), index=False)
    post.to_csv(deba.data("fuse_agency/post_kenner_pd.csv"), index=False)
