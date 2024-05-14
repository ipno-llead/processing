import deba
from lib.columns import (
    rearrange_allegation_columns,
)
from lib import events
from lib.personnel import fuse_personnel
from lib.post import load_for_agency
import pandas as pd


def fuse_events(post, cprr15, cprr10):
    builder = events.Builder()
    builder.extract_events(
        cprr15,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "allegation_uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr10,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation_uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        post,
        {
            events.OFFICER_LEVEL_1_CERT: {
                "prefix": "level_1_cert",
                "parse_date": "%Y-%m-%d",
                "keep": ["uid", "agency", "employment_status"],
            },
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": ["uid", "agency", "employment_status"],
            },
            events.OFFICER_PC_12_QUALIFICATION: {
                "prefix": "last_pc_12_qualification",
                "parse_date": "%Y-%m-%d",
                "keep": [
                    "uid",
                    "agency",
                    "employment_status",
                ],
            },
        },
        ["uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr21 = pd.read_csv(deba.data("match/cprr_lafourche_so_2019_2021.csv"))
    cprr15 = pd.read_csv(deba.data("match/cprr_lafourche_so_2015_2018.csv"))
    cprr10 = pd.read_csv(deba.data("match/cprr_lafourche_so_2010_2014.csv"))
    cprr22 = pd.read_csv(deba.data("match/cprr_lafourche_so_2022_2023.csv"))
    agency = cprr21.agency[0]
    post = load_for_agency(agency)
    per_df = fuse_personnel(cprr21, post, cprr15, cprr10, cprr22)
    com_df = rearrange_allegation_columns(pd.concat([cprr21, cprr15, cprr10, cprr22], axis=0))
    event_df = fuse_events(post, cprr15, cprr10)
    event_df.to_csv(deba.data("fuse_agency/event_lafourche_so.csv"), index=False)
    com_df.to_csv(deba.data("fuse_agency/com_lafourche_so.csv"), index=False)
    per_df.to_csv(deba.data("fuse_agency/per_lafourche_so.csv"), index=False)
