import pandas as pd
import deba
from lib import events
from lib.columns import rearrange_allegation_columns
from lib.personnel import fuse_personnel
from lib.post import load_for_agency


def fuse_events(cprr, cprr19, pprr, post):
    builder = events.Builder()
    builder.extract_events(
        cprr,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
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
                "keep": ["uid", "agency", "employment_status"],
            },
        },
        ["uid"],
    )
    builder.extract_events(
        pprr,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": [
                    "uid",
                    "agency",
                    "employee_id",
                    "rank_desc",
                    "salary",
                    "salary_freq",
                ],
            }
        },
        ["uid"],
    )
    builder.extract_events(
        cprr19,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["uid", "agency", "allegation_uid"],
            },
        },
        ["uid", "allegation_uid"],
    )
    return builder.to_frame()

if __name__ == "__main__":
    cprr = pd.read_csv(deba.data("match/cprr_caddo_so_2022_2023.csv"))
    cprr19 = pd.read_csv(deba.data("match/cprr_caddo_so_2015_2019.csv"))
    pprr = pd.read_csv(deba.data("match/pprr_caddo_parish_so_2020.csv"))
    agency = cprr.agency[0]
    post = load_for_agency(agency)
    per = fuse_personnel(cprr,cprr19,post,pprr)
    combined_cprr = pd.concat([cprr, cprr19])
    com = rearrange_allegation_columns(combined_cprr)
    event = fuse_events(cprr,cprr19,pprr,post)
    event.to_csv(deba.data("fuse_agency/event_caddo_so.csv"), index=False)
    com.to_csv(deba.data("fuse_agency/com_caddo_so.csv"), index=False)
    per.to_csv(deba.data("fuse_agency/per_caddo_so.csv"), index=False)
