import re
import sys

import pandas as pd

from lib.columns import clean_column_names
from lib.path import data_file_path, ensure_data_dir
from lib.clean import clean_dates, clean_salaries, clean_names

sys.path.append('../')


def realign(df):
    pat = re.compile(r"^([^\d]*)(\d{2}-\d{4})?([^\d]*)$")
    pat2 = re.compile(r"^(.+, \w{2,}(?: \w)?) (\w{2,}.*)$")
    rank_whitelist = set([
        'captain', 'police officer', 'i/t coordinator',
        'assistant chief of police', 'it technician', 'sergeant',
        'jailer / dispatch provisional', 'police lieutenant',
        'probational police officer', 'secretary',
        'jailer / dispatch probational', 'police mechanic', 'police sgt.',
        'police property manager', 'part time police officer', 'jailer',
        'jailer/dispatch', 'provisional sergeant',
        'records clerk', 'police captain', 'police chief secretary',
        'assist chief of police', 'police dept. agent',
        'provisional chief secretary', 'assistant police chief',
        'police records clerk', 'maintenance man', 'police jailer',
        'police sgt', 'mechanic', 'police m/c mechanic',
        'dispatch / jailer', 'police sergeant', 'police officer sgt',
        'police chief', 'secretary - police chief'])
    record = dict()
    records = []
    for _, row in df.iterrows():
        match = pat.match(row[0])
        rank = (match.group(1) or "").lower().strip()
        emp_no = match.group(2) or ""
        name = (match.group(3) or "").lower().strip()
        if rank != "":
            record["rank"] = rank
            rank = ""
        if emp_no != "":
            if len(record) > 0:
                records.append(record)
                record = dict()
            record["emp_no"] = int(emp_no[3:])
        if name != "":
            record["name"] = name
            match2 = pat2.match(name)
            if match2 is not None and match2.group(2).lower() in rank_whitelist:
                record["name"] = match2.group(1)
                record["rank"] = match2.group(2)
        if row[1] != "":
            record["dept_no"] = row[1].strip()
            record["annual"] = row[2]
            record["status"] = row[3]

    if len(record) > 0:
        records.append(record)
        record = dict()

    return pd.DataFrame.from_records(records)


def split_names(df):
    names = df.name.str.replace(r', (jr\.|iii)(?:,| \.)', r' \1,', regex=True)\
        .str.extract(r'^([^,]+), (\w+)(?: (\w+))?$')
    df.loc[:, 'first_name'] = names[1]
    df.loc[:, 'middle_name'] = names[2].fillna('')
    df.loc[:, 'middle_initial'] = df.middle_name.map(lambda x: x[:1])
    df.loc[:, 'last_name'] = names[0]
    return df.drop(columns=['name'])


def clean_employment_status(df):
    df.loc[:, 'employment_status'] = df.employment_status.str.lower().str.strip()\
        .str.replace(r'^(i|t)$', 'inactive', regex=True)\
        .str.replace(r'^a$', 'active', regex=True)
    return df


def assign_agency(df):
    df.loc[:, 'data_production_year'] = 2020
    df.loc[:, 'agency'] = 'Harahan CSD'
    return df


def clean():
    return pd.read_csv(data_file_path(
        'harahan_csd/harahan_csd_pprr_roster_by_employment_status_2020.csv'
    )).pipe(clean_column_names)\
        .fillna("")\
        .pipe(realign)\
        .rename(columns={
            'annual': 'annual_salary',
            'emp_no': 'employee_id',
            'status': 'employment_status'
        })\
        .drop(columns=['dept_no'])\
        .pipe(split_names)\
        .pipe(clean_employment_status)\
        .pipe(clean_salaries, ['annual_salary'])\
        .pipe(clean_names, ['first_name', 'middle_name', 'middle_initial', 'last_name'])\
        .pipe(join_employment_date)\
        .pipe(assign_agency)


def join_employment_date(df):
    emp_dates = pd.read_csv(data_file_path(
        'harahan_csd/harahan_csd_prrr_roster_by_employment_date_2020.csv'
    )).pipe(clean_column_names)\
        .drop(columns=['employee_s_name', 'status', 'p_f_dept'])\
        .rename(columns={
            'emp_no': 'employee_id',
            'hire': 'hire_date',
            'termination': 'left_date'
        })\
        .pipe(clean_dates, ['hire_date'])
    return df.merge(emp_dates, on='employee_id')


if __name__ == '__main__':
    df = clean()
    ensure_data_dir('clean')
    df.to_csv(data_file_path(
        'clean/pprr_harahan_csd.csv'
    ), index=False)
