import sys

import pandas as pd

from lib.columns import clean_column_names, set_values
from lib.path import data_file_path, ensure_data_dir
from lib.clean import clean_names
from lib.uid import gen_uid

sys.path.append("../")


def remove_header_rows(df):
    return df[
        df.date_received.str.strip() != "Date\rReceived"
    ].reset_index(drop=True)


def remove_carriage_return(df, cols):
    for col in cols:
        df.loc[:, col] = df[col].str.replace(r"-\r", r"-")\
            .str.replace(r"\r", r" ")
    return df


def split_name(df):
    series = df.name_of_accused.fillna('').str.strip()
    for col, pat in [('first_name', r"^([\w'-]+)(.*)$"), ('middle_initial', r'^(\w\.) (.*)$')]:
        names = series[series.str.match(pat)].str.extract(pat)
        df.loc[series.str.match(pat), col] = names[0]
        series = series.str.replace(pat, r'\2').str.strip()
    df.loc[:, 'last_name'] = series
    return df.drop(columns=['name_of_accused'])


def clean_action(df):
    df.loc[:, 'action'] = df.action.str.strip()\
        .str.replace(r'\s+', ' ', regex=True)\
        .str.replace('RUI', 'Released under investigation', regex=False)\
        .str.replace('RTD', 'Return to duty', regex=False)\
        .str.replace('DRB', 'Disciplinary review board', regex=False)\
        .str.replace('IAD', 'Internal affairs department', regex=False)
    return df


def process_disposition(df):
    df.loc[:, 'disposition'] = df.disposition.str.strip()
    df.loc[:, 'charges'] = df.charges.str.strip()\
        .str.replace(r',$', r'', regex=True)
    for _, row in df.iterrows():
        if pd.isnull(row.disposition) or pd.isnull(row.charges):
            continue
        charges = row.charges.lower()
        if row.disposition.lower().startswith(charges):
            row.disposition = row.disposition[len(charges):]
    df.loc[:, 'disposition'] = df.disposition.str.strip()\
        .str.replace(r'^[- ]+', '', regex=True)
    return df


def clean_receive_date(df):
    df.loc[:, 'receive_date'] = df.receive_date.str.strip()\
        .str.replace(r'2011(\d)$', r'201\1', regex=True)
    return df


def clean():
    df = pd.read_csv(data_file_path(
        "new_orleans_so/new_orleans_so_cprr_2019_tabula.csv"))
    df = clean_column_names(df)
    return df\
        .pipe(remove_header_rows)\
        .drop(columns=[
            'month', 'quarter', 'numb_er_of_cases', 'related_item_number', 'a_i',
            'intial_action', 'inmate_grievance', 'referred_by', 'date_of_board'
        ])\
        .rename(columns={
            'date_received': 'receive_date',
            'case_number': 'tracking_number',
            'job_title': 'rank_desc',
            'charge_disposition': 'disposition',
            'location_or_facility': 'department_desc',
            'assigned_agent': 'investigating_supervisor',
            'emp_no': 'employee_id',
            'date_started': 'investigation_start_date',
            'date_completed': 'investigation_complete_date',
            'terminated_resigned': 'action'
        })\
        .pipe(remove_carriage_return, [
            'name_of_accused', 'disposition', 'charges', 'summary', 'investigating_supervisor',
            'action'
        ])\
        .pipe(split_name)\
        .pipe(clean_names, ['first_name', 'last_name', 'middle_initial'])\
        .pipe(clean_action)\
        .pipe(set_values, {
            'agency': 'New Orleans SO',
            'data_production_year': '2019'
        })\
        .pipe(process_disposition)\
        .pipe(clean_receive_date)\
        .pipe(gen_uid, ['agency', 'employee_id', 'first_name', 'last_name', 'middle_initial'])\
        .pipe(gen_uid, ['agency', 'tracking_number'], 'complaint_uid')\
        .pipe(gen_uid, ['complaint_uid', 'uid'], 'allegation_uid')\
        .sort_values(['tracking_number', 'investigation_complete_date'])\
        .drop_duplicates(subset=['allegation_uid'], keep='last', ignore_index=True)


if __name__ == '__main__':
    df = clean()
    ensure_data_dir('clean')
    df.to_csv(data_file_path('clean/cprr_new_orleans_so_2019.csv'), index=False)
