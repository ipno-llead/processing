import pandas as pd
import deba
from lib import events
from lib.personnel import fuse_personnel
from lib.columns import rearrange_allegation_columns
from lib.post import load_for_agency


def fuse_events(post):
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
    return builder.to_frame()


if __name__ == "__main__":
    cprr21 = pd.read_csv(deba.data("match/cprr_houma_pd_2019_2021.csv"))
    cprr18 = pd.read_csv(deba.data("match/cprr_houma_pd_2017_2018.csv"))
    agency = cprr18.agency[0]
    post = load_for_agency(agency)
    personnel_df = fuse_personnel(cprr21, cprr18, post)
    allegation_df = rearrange_allegation_columns(pd.concat([cprr21, cprr18]))
    event_df = fuse_events(post)
    event_df.to_csv(deba.data("fuse/event_houma_pd.csv"), index=False)
    personnel_df.to_csv(deba.data("fuse/per_houma_pd.csv"), index=False)
    allegation_df.to_csv(deba.data("fuse/com_houma_pd.csv"), index=False)
