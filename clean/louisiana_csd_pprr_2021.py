from lib.columns import clean_column_names, set_values
from lib.path import data_file_path, ensure_data_dir
from lib.clean import (
    clean_races, clean_sexes, clean_salaries, clean_names
)
from lib.uid import gen_uid
from lib import salary
import pandas as pd
import numpy as np
import sys
sys.path.append('../')


def split_names(df):
    names = df.employee_name.str.strip().str.extract(
        r'^([^,]+),([^ ]+)(?: (\w+))?$')
    df.loc[:, 'last_name'] = names.loc[:, 0]
    df.loc[:, 'first_name'] = names.loc[:, 1]
    df.loc[:, 'middle_name'] = names.loc[:, 2]
    df.loc[:, 'middle_initial'] = df.middle_name.fillna('').map(lambda x: x[:1])
    return df.drop(columns=['employee_name'])


def convert_hire_date(df):
    df.loc[:, 'hire_date'] = df.hire_date.astype(str).map(
        lambda x: np.NaN if x == '0' else x
    ).str.replace(r'(\d{4})(\d{2})(\d{2})', r'\2/\3/\1', regex=True)
    return df


def clean_rank_desc(df):
    df.loc[:, 'rank_desc'] = df.rank_desc.str.lower().str.strip()\
        .str.replace(r'^(state )?police ', '', regex=True)\
        .str.replace(r'troop$', r'trooper', regex=True)\
        .str.replace(r'\b(supt\b|superintende$)', r'superintendent', regex=True)\
        .str.replace(r'\bsvc\b', 'services', regex=True)\
        .str.replace(r'\bemer\b', 'emergency', regex=True)\
        .str.replace(r'\bdep\b', 'deputy', regex=True)\
        .str.replace(r'\btec\b', 'technician', regex=True)\
        .str.replace(r'\bcom\b', 'communications', regex=True)\
        .str.replace('op plan & tr', 'operations planning and training', regex=False)
    return df


def clean_department_desc(df):
    df.loc[:, 'department_desc'] = df.department_desc.str.lower().str.strip()\
        .str.replace(r'\bcrim\b', 'crime', regex=True)\
        .str.replace(r'\binv div\b', 'investigation division', regex=True)\
        .str.replace(r'\bsvcs\b', 'services', regex=True)\
        .str.replace(r'\bsuppt?\b', 'support', regex=True)\
        .str.replace(r'\bpublic saf\b', 'public safety', regex=True)\
        .str.replace(r'\btech sup\b', 'technical support', regex=True)\
        .str.replace(r'\bweights & (stds|standa?)\b', 'weights & standards', regex=True)\
        .str.replace(r'\badmi(ni)?\b', 'administration', regex=True)\
        .str.replace(r'\btowing & rec(ove?)?\b', 'towing & recovery', regex=True)\
        .str.replace(r'\btrans & env\b', 'transport & environment', regex=True)\
        .str.replace(r'\bla st p(olice)?\b', 'lsp', regex=True)\
        .str.replace(r'\banti-tam\b', 'anti-tampering', regex=True)\
        .str.replace(r'\bspec invs\b', 'special investigations', regex=True)\
        .str.replace(r'\bchild exp(loit)?\b', 'child exploitation', regex=True)\
        .str.replace(r'\basset forfeit\b', 'asset forfeiture', regex=True)\
        .str.replace(r'\bplanning & trg\b', 'planning & training', regex=True)

    return df


def clean_pprr_demographic():
    return pd.read_csv(data_file_path(
        'louisiana_csd/louisiana_csd_pprr_demographics_2021.csv'
    )).pipe(clean_column_names)\
        .drop(columns=['agency_name', 'classified_unclassified', 'data_date'])\
        .rename(columns={
            'organizational_unit': 'department_desc',
            'job_title': 'rank_desc',
            'annual_rate_of_pay': 'salary',
            'gender': 'sex'
        })\
        .pipe(set_values, {
            'agency': 'Louisiana State Police',
            'data_production_year': '2021',
            'salary_freq': salary.YEARLY
        })\
        .pipe(convert_hire_date)\
        .pipe(clean_salaries, ['salary'])\
        .pipe(clean_races, ['race'])\
        .pipe(clean_sexes, ['sex'])\
        .pipe(clean_rank_desc)\
        .pipe(clean_department_desc)\
        .pipe(split_names)\
        .pipe(clean_names, ['first_name', 'last_name', 'middle_name',
                            'middle_initial'])\
        .pipe(gen_uid, ['agency', 'first_name', 'last_name',
                        'middle_name', 'race', 'sex'])


def clean_left_reason(df):
    df.loc[:, 'left_reason'] = df.left_reason.str.lower().str.strip()\
        .replace({
            'term of temp appt': 'termination of temporary appointment'
        }).str.replace(r'^resign-.+', 'resign', regex=True)
    return df


def clean_pprr_termination():
    return pd.read_csv(data_file_path(
        'louisiana_csd/louisiana_csd_pprr_terminations_2021.csv'
    )).pipe(clean_column_names)\
        .drop(columns=['agency_name', 'action_type'])\
        .rename(columns={
            'organization_unit': 'department_desc',
            'job_title': 'rank_desc',
            'action_effective_date': 'left_date',
            'action_reason': 'left_reason'
        })\
        .pipe(clean_left_reason)\
        .pipe(clean_rank_desc)\
        .pipe(clean_department_desc)\
        .pipe(clean_names, ['first_name', 'last_name'])\
        .pipe(set_values, {
            'agency': 'Louisiana State Police',
            'data_production_year': '2021',
        })\
        .pipe(gen_uid, ['agency', 'first_name', 'last_name'])


if __name__ == '__main__':
    dfd = clean_pprr_demographic()
    dft = clean_pprr_termination()
    ensure_data_dir('clean')
    dfd.to_csv(data_file_path(
        'clean/pprr_louisiana_csd_2021.csv'
    ), index=False)
    dft.to_csv(data_file_path(
        'clean/pprr_term_louisiana_csd_2021.csv'
    ))
