import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.personnel import fuse_personnel
from lib.columns import (
    rearrange_event_columns, rearrange_complaint_columns,
    rearrange_appeal_hearing_columns
)
from lib.uid import ensure_uid_unique
from lib import events
import sys
sys.path.append("../")


def fuse_events(csd_pprr_17, csd_pprr_19, pd_cprr_18, lprr):
    builder = events.Builder()

    builder.extract_events(csd_pprr_17, {
        events.OFFICER_HIRE: {'prefix': 'hire', 'keep': ['uid', 'agency']},
        events.OFFICER_LEFT: {'prefix': 'resign', 'keep': ['uid', 'agency']},
        events.OFFICER_PAY_EFFECTIVE: {'prefix': 'pay_effective', 'keep': [
            'uid', 'agency', 'hourly_salary', 'yearly_salary']},
        events.OFFICER_RANK: {'prefix': 'rank', 'keep': [
            'uid', 'agency', 'rank_code', 'rank_desc']},
        events.OFFICER_DEPT: {'prefix': 'dept', 'keep': [
            'uid', 'agency', 'department_code', 'department_desc']},
    })
    builder.extract_events(csd_pprr_19, {
        events.OFFICER_HIRE: {'prefix': 'hire', 'keep': ['uid', 'agency']},
        events.OFFICER_LEFT: {'prefix': 'resign', 'keep': ['uid', 'agency']},
        events.OFFICER_PAY_EFFECTIVE: {'prefix': 'pay_effective', 'keep': ['uid', 'agency', 'yearly_salary']},
        events.OFFICER_RANK: {'prefix': 'rank', 'keep': ['uid', 'agency', 'rank_code', 'rank_desc']},
        events.OFFICER_DEPT: {'prefix': 'dept', 'keep': ['uid', 'agency', 'department_code', 'department_desc']},
    })
    builder.extract_events(pd_cprr_18, {
        events.COMPLAINT_RECEIVE: {'prefix': 'receive', 'keep': ['uid', 'agency', 'complaint_uid']},
        events.COMPLAINT_INCIDENT: {'prefix': 'occur', 'keep': ['uid', 'agency', 'complaint_uid']},
    })
    builder.extract_events(lprr, {
        events.APPEAL_HEARING: {'prefix': 'hearing', 'keep': ['uid', 'agency', 'appeal_uid']},
    })
    return builder.to_frame(
        ["kind", "year", "month", "day", "uid", "complaint_uid", "appeal_uid", "department_code"], True)


if __name__ == "__main__":
    csd_pprr_17 = pd.read_csv(
        data_file_path("match/pprr_baton_rouge_csd_2017.csv")
    )
    csd_pprr_19 = pd.read_csv(
        data_file_path("match/pprr_baton_rouge_csd_2019.csv")
    )
    csd_pprr_17.loc[:, 'agency'] = 'Baton Rouge PD'
    csd_pprr_19.loc[:, 'agency'] = 'Baton Rouge PD'
    pd_cprr_18 = pd.read_csv(
        data_file_path("match/cprr_baton_rouge_pd_2018.csv"))
    lprr = pd.read_csv(data_file_path(
        "match/lprr_baton_rouge_fpcsb_1992_2012.csv"))
    post_event = pd.read_csv(data_file_path(
        'match/event_post_baton_rouge_pd.csv'))
    personnel_df = fuse_personnel(
        csd_pprr_17.drop_duplicates(subset='uid', keep='last'),
        csd_pprr_19, pd_cprr_18, lprr)
    events_df = fuse_events(csd_pprr_17, csd_pprr_19, pd_cprr_18, lprr)
    events_df = rearrange_event_columns(pd.concat([
        post_event,
        events_df
    ]))
    ensure_uid_unique(events_df, 'event_uid', True)

    complaint_df = rearrange_complaint_columns(pd_cprr_18)
    lprr_df = rearrange_appeal_hearing_columns(lprr)

    ensure_data_dir("fuse")
    personnel_df.to_csv(data_file_path(
        "fuse/per_baton_rouge_pd.csv"), index=False)
    events_df.to_csv(data_file_path(
        "fuse/event_baton_rouge_pd.csv"), index=False)
    complaint_df.to_csv(data_file_path(
        "fuse/com_baton_rouge_pd.csv"), index=False)
    lprr_df.to_csv(data_file_path("fuse/app_baton_rouge_pd.csv"), index=False)
