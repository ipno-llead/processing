import pandas as pd
import bolo
from lib.columns import rearrange_allegation_columns
from lib.personnel import fuse_personnel
from lib.post import load_for_agency
from lib import events


def fuse_events(cprr_18, cprr_21, post):
    builder = events.Builder()
    builder.extract_events(
        cprr_18,
        {
            events.OFFICER_RANK: {
                "prefix": "rank",
                "keep": ["uid", "agency", "badge_no", "rank_desc"],
                "id_cols": ["uid"],
            },
            events.COMPLAINT_INCIDENT: {
                "prefix": "occur",
                "keep": ["uid", "agency", "allegation_uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        cprr_21,
        {
            events.OFFICER_RANK: {
                "prefix": "rank",
                "keep": ["uid", "agency", "badge_no", "rank_desc"],
                "id_cols": ["uid"],
            },
            events.COMPLAINT_INCIDENT: {
                "prefix": "occur",
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
                "keep": ["uid", "agency"],
            },
            events.OFFICER_PC_12_QUALIFICATION: {
                "prefix": "last_pc_12_qualification",
                "parse_date": "%Y-%m-%d",
                "keep": ["uid", "agency"],
            },
            events.OFFICER_HIRE: {"prefix": "hire", "keep": ["uid", "agency"]},
        },
        ["uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr_18 = pd.read_csv(bolo.data("match/cprr_baton_rouge_so_2018.csv"))
    cprr_20 = pd.read_csv(bolo.data("match/cprr_baton_rouge_so_2016_2020.csv"))
    agency = cprr_20.agency[0]
    post = load_for_agency(agency)
    personnel_df = fuse_personnel(cprr_18, cprr_20, post)
    event_df = fuse_events(cprr_18, cprr_20, post)
    complaint_df = rearrange_allegation_columns(pd.concat([cprr_18, cprr_20]))
    personnel_df.to_csv(bolo.data("fuse/per_baton_rouge_so.csv"), index=False)
    event_df.to_csv(bolo.data("fuse/event_baton_rouge_so.csv"), index=False)
    complaint_df.to_csv(bolo.data("fuse/com_baton_rouge_so.csv"), index=False)
