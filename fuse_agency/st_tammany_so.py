import deba
from lib.columns import rearrange_allegation_columns, rearrange_citizen_columns, rearrange_event_columns
from lib.personnel import fuse_personnel
from lib import events
import pandas as pd
from lib.post import load_for_agency


def fuse_events(pprr, cprr, pprr26, uof25):
    builder = events.Builder()
    builder.extract_events(
        pprr,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": ["uid", "agency", "rank_code", "rank_desc", "pay_group"],
            },
            events.OFFICER_LEFT: {
                "prefix": "term",
                "keep": ["uid", "agency", "rank_code", "rank_desc", "pay_group"],
            },
        },
        ["uid"],
    )
    builder.extract_events(
        cprr,
        {
            events.COMPLAINT_INCIDENT: {
                "prefix": "occur",
                "keep": ["uid", "agency", "allegation_uid"],
            }
        },
        ["uid", "allegation_uid"],
    )
    pprr26_only = pprr26[~pprr26.uid.isin(pprr.uid)]
    builder.extract_events(
    pprr26_only,
    {
        events.OFFICER_HIRE: {
            "prefix": "hire",
            "keep": ["uid", "agency", "rank_desc"],
        },
        events.OFFICER_LEFT: {
            "prefix": "inactive",
            "keep": ["uid", "agency", "rank_desc"],
        },
    },
    ["uid"],
    )
    builder.extract_events(
        uof25,
        {
            events.UOF_INCIDENT: {
                "prefix": "occurred",
                #"parse_date": True,
                "keep": [
                    "uid",
                    "uof_uid",
                    "agency",
                    "use_of_force_type"
                ],
            },
        },
        ["uid", "uof_uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr = pd.read_csv(deba.data("match/cprr_st_tammany_so_2011_2021.csv"))
    pprr = pd.read_csv(deba.data("match/pprr_st_tammany_so_2020.csv"))
    pprr26 = pd.read_csv(deba.data("match/pprr_st_tammany_so_2026.csv"))
    uof25 = pd.read_csv(deba.data("match/uof_st_tammany_so_2025.csv"))
    citizen_df = pd.read_csv(deba.data("clean/uof_cit_st_tammany_so_2025.csv"))
    #combined_cit = pd.concat([citizen_df, citizen_22_df], ignore_index=True)
    citizen_df = rearrange_citizen_columns(citizen_df)
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    post_event = pd.read_csv(deba.data("match/post_event_st_tammany_so_2020.csv"))
    personnels = fuse_personnel(pprr, cprr, post, pprr26, uof25)
    complaints = rearrange_allegation_columns(cprr)
    events_df = fuse_events(pprr, cprr, pprr26, uof25)
    events_df = rearrange_event_columns(pd.concat([post_event, events_df]))
    personnels.to_csv(deba.data("fuse_agency/per_st_tammany_so.csv"), index=False)
    events_df.to_csv(deba.data("fuse_agency/event_st_tammany_so.csv"), index=False)
    complaints.to_csv(deba.data("fuse_agency/com_st_tammany_so.csv"), index=False)
    post.to_csv(deba.data("fuse_agency/post_st_tammany_so.csv"), index=False)
    uof25.to_csv(deba.data("fuse_agency/uof_st_tammany_so.csv"), index=False)
    citizen_df.to_csv(deba.data("fuse_agency/cit_st_tammany_so.csv"), index=False)
    
