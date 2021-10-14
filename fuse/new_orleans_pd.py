import pandas as pd
from lib.path import data_file_path
from lib.columns import (
    rearrange_appeal_hearing_columns, rearrange_complaint_columns,
    rearrange_use_of_force, rearrange_event_columns)
from lib.clean import float_to_int_str
from lib.personnel import fuse_personnel
from lib.uid import ensure_uid_unique
from lib import events

import sys
sys.path.append("../")


def create_officer_number_dict(pprr):
    df = pprr[['employee_id', 'uid']]
    df.loc[:, 'employee_id'] = df.employee_id.astype(str)
    return df.set_index('employee_id').uid.to_dict()


def fuse_cprr(cprr, actions, officer_number_dict):
    # actions.loc[:, 'allegation_primary_key'] = actions.allegation_primary_key\
    #     .astype(str)
    # actions_dict = actions.set_index('allegation_primary_key').action.to_dict()
    cprr = float_to_int_str(
        cprr, ['officer_primary_key', 'allegation_primary_key'])
    cprr.loc[:, 'uid'] = cprr.officer_primary_key.map(
        lambda x: officer_number_dict.get(x, ''))
    # cprr.loc[:, 'action'] = cprr.allegation_primary_key.map(
    #     lambda x: actions_dict.get(x, ''))
    return rearrange_complaint_columns(cprr)


def fuse_use_of_force(uof, officer_number_dict):
    uof = float_to_int_str(uof, ['officer_primary_key'])
    uof.loc[:, 'uid'] = uof.officer_primary_key.map(
        lambda x: officer_number_dict.get(x, ''))
    return rearrange_use_of_force(uof)


def fuse_events(pprr, cprr, uof, award, lprr):
    builder = events.Builder()
    builder.extract_events(pprr, {
        events.OFFICER_HIRE: {'prefix': 'hire'},
        events.OFFICER_LEFT: {'prefix': 'left'},
        events.OFFICER_DEPT: {'prefix': 'dept'},
    }, ['uid'])
    builder.extract_events(cprr, {
        events.COMPLAINT_RECEIVE: {'prefix': 'receive'},
        events.ALLEGATION_CREATE: {'prefix': 'allegation_create'},
        events.COMPLAINT_INCIDENT: {'prefix': 'occur'},
    }, ['uid', 'complaint_uid'])
    builder.extract_events(uof, {
        events.UOF_INCIDENT: {'prefix': 'occur'},
        events.UOF_RECEIVE: {'prefix': 'receive', 'parse_date': True},
        events.UOF_ASSIGNED: {'prefix': 'assigned', 'parse_date': True},
        events.UOF_COMPLETED: {'prefix': 'completed', 'parse_date': True},
        events.UOF_CREATED: {'prefix': 'created', 'parse_date': True},
        events.UOF_DUE: {'prefix': 'due', 'parse_datetime': True},
    }, ['uid', 'uof_uid'])
    builder.extract_events(award, {
        events.AWARD_RECEIVE: {
            'prefix': 'receive',
            'parse_date': True,
            'keep': ['uid', 'agency', 'award'],
        },
        events.AWARD_RECOMMENDED: {
            'prefix': 'recommendation',
            'parse_date': True,
            'keep': ['uid', 'agency', 'recommended_award'],
        },
    }, ['uid', 'award'])
    builder.extract_events(lprr, {
        events.APPEAL_DISPOSITION: {
            'prefix': 'appeal_disposition',
            'keep': ['uid', 'agency', 'appeal_uid'],
        },
        events.APPEAL_RECEIVE: {
            'prefix': 'appeal_receive',
            'keep': ['uid', 'agency', 'appeal_uid']
        },
        events.APPEAL_HEARING: {
            'prefix': 'appeal_hearing',
            'keep': ['uid', 'agency', 'appeal_uid']
        },
        events.APPEAL_HEARING_2: {
            'prefix': 'appeal_hearing_2',
            'keep': ['uid', 'agency', 'appeal_uid']
        }
    }, ['uid', 'appeal_uid'])
    return builder.to_frame()


if __name__ == "__main__":
    pprr = pd.read_csv(data_file_path(
        'clean/pprr_new_orleans_pd_1946_2018.csv'
    ))
    officer_number_dict = create_officer_number_dict(pprr)
    cprr = pd.read_csv(data_file_path(
        'clean/cprr_new_orleans_pd_1931_2020.csv'
    ))
    actions = pd.read_csv(data_file_path(
        'clean/cprr_actions_new_orleans_pd_1931_2020.csv'
    ))
    uof = pd.read_csv(data_file_path(
        'clean/uof_new_orleans_pd_2012_2019.csv'
    ))
    post_event = pd.read_csv(data_file_path(
        'match/post_event_new_orleans_pd.csv'))
    award = pd.read_csv(data_file_path(
        'match/award_new_orleans_pd_2016_2021.csv'
    ))
    lprr = pd.read_csv(data_file_path(
        'match/lprr_new_orleans_csc_2000_2016.csv'
    ))
    complaints = fuse_cprr(cprr, actions, officer_number_dict)
    ensure_uid_unique(complaints, 'complaint_uid', output_csv=True)
    use_of_force = fuse_use_of_force(uof, officer_number_dict)
    personnel = fuse_personnel(pprr, lprr)
    events_df = fuse_events(pprr, cprr, uof, award, lprr)
    events_df = rearrange_event_columns(pd.concat([
        post_event,
        events_df
    ]))
    lprr_df = rearrange_appeal_hearing_columns(lprr)
    complaints.to_csv(data_file_path(
        'fuse/com_new_orleans_pd.csv'), index=False)
    use_of_force.to_csv(data_file_path(
        'fuse/uof_new_orleans_pd.csv'), index=False)
    personnel.to_csv(data_file_path(
        'fuse/per_new_orleans_pd.csv'), index=False)
    events_df.to_csv(data_file_path(
        'fuse/event_new_orleans_pd.csv'), index=False)
    lprr_df.to_csv(data_file_path(
        'fuse/lprr_new_orleans_csc.csv'), index=False)
