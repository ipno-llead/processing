from lib.personnel import fuse_personnel

import pandas as pd

import deba
from lib.columns import rearrange_allegation_columns, rearrange_event_columns, rearrange_use_of_force, rearrange_citizen_columns
from lib import events
from lib.post import load_for_agency


def fuse_events(cprr_25, cprr_20, cprr_14, pprr, uof):
    builder = events.Builder()
    builder.extract_events(
        cprr_25,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                #"parse_date": True,
                "keep": ["agency", "allegation_uid", "uid", "invetigator_uid"],
            },
            events.INVESTIGATION_COMPLETE: {
                "prefix": "complete",
                #"parse_date": True,
                "ignore_bad_date": True,
                "keep": ["agency", "allegation_uid", "uid", "invetigator_uid"],
            },
        },
        ["allegation_uid"],
    )
    builder.extract_events(
        cprr_20,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "parse_date": True,
                "keep": ["agency", "allegation_uid", "uid", "invetigator_uid"],
            },
            events.INVESTIGATION_COMPLETE: {
                "prefix": "complete",
                "parse_date": True,
                "ignore_bad_date": True,
                "keep": ["agency", "allegation_uid", "uid", "invetigator_uid"],
            },
        },
        ["allegation_uid"],
    )
    builder.extract_events(
        cprr_14,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["agency", "allegation_uid", "uid", "invetigator_uid"],
            },
            events.INVESTIGATION_COMPLETE: {
                "prefix": "complete",
                "keep": ["agency", "allegation_uid", "uid", "invetigator_uid"],
            },
        },
        ["allegation_uid"],
    )
    builder.extract_events(
        pprr,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "parse_date": True,
                "keep": ["agency", "uid", "salary", "salary_freq", "rank_desc"],
            },
            events.OFFICER_LEFT: {
                "prefix": "left",
                "parse_date": True,
                "keep": ["agency", "uid", "salary", "salary_freq", "rank_desc"],
            },
            events.OFFICER_LEFT: {
                "prefix": "termination",
                "parse_date": True,
                "keep": ["agency", "uid", "salary", "salary_freq", "rank_desc"],
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
    cprr_25 = pd.read_csv(deba.data("match/cprr_lafayette_pd_2020_2025.csv"))
    cprr_20 = pd.read_csv(deba.data("match/cprr_lafayette_pd_2015_2020.csv"))
    cprr_14 = pd.read_csv(deba.data("match/cprr_lafayette_pd_2009_2014.csv"))
    uof = pd.read_csv(deba.data("match/uof_lafayette_pd_2024_2026.csv"))
    uof_cit = pd.read_csv(deba.data("clean/uof_cit_lafayette_pd_2024_2026.csv"))
    pprr = pd.read_csv(deba.data("clean/pprr_lafayette_pd_2010_2024.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    post_events = pd.read_csv(deba.data("match/post_event_lafayette_pd_2025.csv"))
    events_df = fuse_events(cprr_25, cprr_20, cprr_14, pprr, uof)
    events_df = rearrange_event_columns(pd.concat([post_events, events_df]))
    per = fuse_personnel(
        post,
        pprr,
        cprr_25[["uid", "first_name", "last_name"]],
        cprr_25[
            ["investigator_uid", "investigator_first_name", "investigator_last_name"]
        ].rename(
            columns={
                "investigator_uid": "uid",
                "investigator_first_name": "first_name",
                "investigator_last_name": "last_name",
            }
        ),
        cprr_20[["uid", "first_name", "last_name"]],
        cprr_20[
            ["investigator_uid", "investigator_first_name", "investigator_last_name"]
        ].rename(
            columns={
                "investigator_uid": "uid",
                "investigator_first_name": "first_name",
                "investigator_last_name": "last_name",
            }
        ),
        cprr_14[["uid", "first_name", "last_name"]],
        cprr_14[
            ["investigator_uid", "investigator_first_name", "investigator_last_name"]
        ].rename(
            columns={
                "investigator_uid": "uid",
                "investigator_first_name": "first_name",
                "investigator_last_name": "last_name",
            }
        ),
        uof[["uid", "first_name", "last_name"]],
    )
    com = rearrange_allegation_columns(pd.concat([cprr_25, cprr_20, cprr_14], axis=0))
    uof_df = rearrange_use_of_force(uof)
    uof_cit = rearrange_citizen_columns(uof_cit)
    per.to_csv(deba.data("fuse_agency/per_lafayette_pd.csv"), index=False)
    com.to_csv(deba.data("fuse_agency/com_lafayette_pd.csv"), index=False)
    events_df.to_csv(deba.data("fuse_agency/event_lafayette_pd.csv"), index=False)
    uof_df.to_csv(deba.data("fuse_agency/uof_lafayette_pd.csv"), index=False)
    uof_cit.to_csv(deba.data("fuse_agency/cit_lafayette_pd.csv"), index=False)
    post.to_csv(deba.data("fuse_agency/post_lafayette_pd.csv"), index=False)
