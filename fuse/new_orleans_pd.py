import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_personnel_columns, rearrange_personnel_history_columns, rearrange_complaint_columns,
    rearrange_use_of_force
)
from lib.clean import float_to_int_str

import sys
sys.path.append("../")


def create_officer_number_dict(pprr):
    df = pprr[['employee_id', 'uid']]
    df.loc[:, 'employee_id'] = df.employee_id.astype(str)
    return df.set_index('employee_id').uid.to_dict()


def fuse_cprr(cprr, actions, officer_number_dict):
    actions.loc[:, 'allegation_primary_key'] = actions.allegation_primary_key.astype(
        str)
    actions_dict = actions.set_index('allegation_primary_key').action.to_dict()
    cprr = float_to_int_str(
        cprr, ['officer_primary_key', 'allegation_primary_key'])
    cprr.loc[:, 'uid'] = cprr.officer_primary_key.map(
        lambda x: officer_number_dict.get(x, ''))
    cprr.loc[:, 'action'] = cprr.allegation_primary_key.map(
        lambda x: actions_dict.get(x, ''))
    return rearrange_complaint_columns(cprr)


def fuse_use_of_force(uof, officer_number_dict):
    uof = float_to_int_str(uof, ['officer_primary_key'])
    uof.loc[:, 'uid'] = uof.officer_primary_key.map(
        lambda x: officer_number_dict.get(x, ''))
    return rearrange_use_of_force(uof)


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
    complaints = fuse_cprr(cprr, actions, officer_number_dict)
    use_of_force = fuse_use_of_force(uof, officer_number_dict)
    personnel = rearrange_personnel_columns(pprr)
    perhist = rearrange_personnel_history_columns(pprr)
    ensure_data_dir("fuse")
    complaints.to_csv(data_file_path(
        'fuse/com_new_orleans_pd.csv'), index=False)
    use_of_force.to_csv(data_file_path(
        'fuse/uof_new_orleans_pd.csv'), index=False)
    personnel.to_csv(data_file_path(
        'fuse/per_new_orleans_pd.csv'), index=False)
    perhist.to_csv(data_file_path(
        'fuse/perhist_new_orleans_pd.csv'), index=False)
