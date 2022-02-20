import pandas as pd
import bolo
from lib.columns import (
    rearrange_appeal_hearing_columns,
    rearrange_allegation_columns,
    rearrange_stop_and_search_columns,
    rearrange_use_of_force,
    rearrange_event_columns,
)
from lib.clean import float_to_int_str
from lib.personnel import fuse_personnel
from lib import events


def create_officer_number_dict(pprr):
    df = pprr[["employee_id", "uid"]]
    df.loc[:, "employee_id"] = df.employee_id.astype(str)
    return df.set_index("employee_id").uid.to_dict()


def fuse_cprr(cprr, actions, officer_number_dict):
    # actions.loc[:, 'allegation_primary_key'] = actions.allegation_primary_key\
    #     .astype(str)
    # actions_dict = actions.set_index('allegation_primary_key').action.to_dict()
    cprr = float_to_int_str(cprr, ["officer_primary_key", "allegation_primary_key"])
    cprr.loc[:, "uid"] = cprr.officer_primary_key.map(
        lambda x: officer_number_dict.get(x, "")
    )
    # cprr.loc[:, 'action'] = cprr.allegation_primary_key.map(
    #     lambda x: actions_dict.get(x, ''))
    return rearrange_allegation_columns(cprr)


def fuse_use_of_force(uof, officer_number_dict):
    uof = float_to_int_str(uof, ["officer_primary_key"])
    uof.loc[:, "uid"] = uof.officer_primary_key.map(
        lambda x: officer_number_dict.get(x, "")
    )
    return rearrange_use_of_force(uof)


def fuse_events(pprr_ipm, pprr_csd, cprr, uof, award, lprr, sas):
    builder = events.Builder()
    builder.extract_events(
        pprr_ipm,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": [
                    "uid",
                    "agency",
                    "rank_code",
                    "rank_desc",
                    "salary",
                    "salary_freq",
                ],
            },
            events.OFFICER_LEFT: {"prefix": "left"},
            events.OFFICER_DEPT: {"prefix": "dept"},
        },
        ["uid"],
    )
    builder.extract_events(
        pprr_csd,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": [
                    "uid",
                    "agency",
                    "rank_code",
                    "rank_desc",
                    "salary",
                    "salary_freq",
                ],
            },
            events.OFFICER_LEFT: {"prefix": "term"},
            events.OFFICER_PAY_PROG_START: {"prefix": "pay_prog_start"},
        },
        ["uid"],
        warn_duplications=True,
    )
    builder.extract_events(
        cprr,
        {
            events.COMPLAINT_RECEIVE: {"prefix": "receive"},
            events.ALLEGATION_CREATE: {"prefix": "allegation_create"},
            events.COMPLAINT_INCIDENT: {"prefix": "occur"},
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        uof,
        {
            events.UOF_INCIDENT: {"prefix": "occur"},
            events.UOF_RECEIVE: {"prefix": "receive", "parse_date": True},
            events.UOF_ASSIGNED: {"prefix": "assigned", "parse_date": True},
            events.UOF_COMPLETED: {"prefix": "completed", "parse_date": True},
            events.UOF_CREATED: {"prefix": "created", "parse_date": True},
            events.UOF_DUE: {"prefix": "due", "parse_datetime": True},
        },
        ["uid", "uof_uid"],
    )
    builder.extract_events(
        award,
        {
            events.AWARD_RECEIVE: {
                "prefix": "receive",
                "parse_date": True,
                "keep": ["uid", "agency", "award"],
            },
            events.AWARD_RECOMMENDED: {
                "prefix": "recommendation",
                "parse_date": True,
                "keep": ["uid", "agency", "recommended_award"],
            },
        },
        ["uid", "award"],
    )
    builder.extract_events(
        lprr,
        {
            events.APPEAL_DISPOSITION: {
                "prefix": "appeal_disposition",
                "parse_date": True,
                "keep": ["uid", "agency", "appeal_uid"],
            },
            events.APPEAL_RECEIVE: {
                "prefix": "appeal_receive",
                "parse_date": True,
                "keep": ["uid", "agency", "appeal_uid"],
            },
            events.APPEAL_HEARING: {
                "prefix": "appeal_hearing",
                "parse_date": True,
                "keep": ["uid", "agency", "appeal_uid"],
            },
            events.APPEAL_HEARING_2: {
                "prefix": "appeal_hearing_2",
                "parse_date": True,
                "keep": ["uid", "agency", "appeal_uid"],
            },
        },
        ["uid", "appeal_uid"],
    )
    builder.extract_events(
        sas,
        {
            events.STOP_AND_SEARCH: {
                "prefix": "stop_and_search",
                "keep": ["uid", "agency", "stop_and_search_uid"],
            }
        },
        ["uid", "stop_and_search_uid"],
    )
    return builder.to_frame(True)


if __name__ == "__main__":
    pprr_ipm = pd.read_csv(bolo.data("clean/pprr_new_orleans_ipm_iapro_1946_2018.csv"))
    pprr_csd = pd.read_csv(bolo.data("match/pprr_new_orleans_csd_2014.csv"))
    officer_number_dict = create_officer_number_dict(pprr_ipm)
    cprr = pd.read_csv(bolo.data("clean/cprr_new_orleans_pd_1931_2020.csv"))
    actions = pd.read_csv(bolo.data("clean/cprr_actions_new_orleans_pd_1931_2020.csv"))
    uof = pd.read_csv(bolo.data("clean/uof_new_orleans_pd_2012_2019.csv"))
    post_event = pd.read_csv(bolo.data("match/post_event_new_orleans_pd.csv"))
    award = pd.read_csv(bolo.data("match/award_new_orleans_pd_2016_2021.csv"))
    lprr = pd.read_csv(bolo.data("match/lprr_new_orleans_csc_2000_2016.csv"))
    sas = pd.read_csv(bolo.data("match/sas_new_orleans_pd_2017_2021.csv"))
    complaints = fuse_cprr(cprr, actions, officer_number_dict)
    use_of_force = fuse_use_of_force(uof, officer_number_dict)
    personnel = fuse_personnel(pprr_ipm, lprr, pprr_csd, sas)
    events_df = fuse_events(pprr_ipm, pprr_csd, cprr, uof, award, lprr, sas)
    events_df = rearrange_event_columns(pd.concat([post_event, events_df]))
    sas_df = rearrange_stop_and_search_columns(sas)
    lprr_df = rearrange_appeal_hearing_columns(lprr)
    complaints.to_csv(bolo.data("fuse/com_new_orleans_pd.csv"), index=False)
    use_of_force.to_csv(bolo.data("fuse/uof_new_orleans_pd.csv"), index=False)
    personnel.to_csv(bolo.data("fuse/per_new_orleans_pd.csv"), index=False)
    events_df.to_csv(bolo.data("fuse/event_new_orleans_pd.csv"), index=False)
    lprr_df.to_csv(bolo.data("fuse/app_new_orleans_csc.csv"), index=False)
    sas_df.to_csv(bolo.data("fuse/sas_new_orleans_pd.csv"), index=False)
