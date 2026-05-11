import pandas as pd
import deba
from lib import events
from lib.personnel import fuse_personnel
from lib.columns import rearrange_allegation_columns, rearrange_event_columns, rearrange_use_of_force
from lib.post import load_for_agency


def fuse_events(post, uof):
    builder = events.Builder()
    builder.extract_events(
        post,
        {
            events.OFFICER_LEVEL_1_CERT: {
                "prefix": "level_1_cert",
                "parse_date": "%Y-%m-%d",
                "keep": ["uid", "agency", "employement_status"],
            },
            events.OFFICER_PC_12_QUALIFICATION: {
                "prefix": "last_pc_12_qualification",
                "parse_date": "%Y-%m-%d",
                "keep": ["uid", "agency", "employment status"],
            },
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": ["uid", "agency", "employment_status"],
            },
        },
        ["uid"],
    )
    builder.extract_events(
        uof,
        {
            events.UOF_INCIDENT: {
                "prefix": "occurred",
                "parse_date": True,
                "keep": ["uid", "uof_uid", "agency"],
            },
        },
        ["uid", "uof_uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr21 = pd.read_csv(deba.data("match/cprr_houma_pd_2019_2021.csv"))
    cprr18 = pd.read_csv(deba.data("match/cprr_houma_pd_2017_2018.csv"))
    uof = pd.read_csv(deba.data("match/uof_houma_pd_2020.csv"))
    agency = cprr18.agency[0]
    post = load_for_agency(agency)
    personnel_df = fuse_personnel(cprr21, cprr18, uof, post)
    allegation_df = rearrange_allegation_columns(pd.concat([cprr21, cprr18], axis=0))
    event_df = rearrange_event_columns(fuse_events(post, uof))
    uof_df = rearrange_use_of_force(uof)
    event_df.to_csv(deba.data("fuse_agency/event_houma_pd.csv"), index=False)
    personnel_df.to_csv(deba.data("fuse_agency/per_houma_pd.csv"), index=False)
    allegation_df.to_csv(deba.data("fuse_agency/com_houma_pd.csv"), index=False)
    uof_df.to_csv(deba.data("fuse_agency/uof_houma_pd.csv"), index=False)
