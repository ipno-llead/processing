import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path
from lib.columns import clean_column_names
from lib.clean import clean_dates
from lib.uid import gen_uid


def extract_department_desc(df):
    df.loc[:, 'actual_position_title'] = df.actual_position_title.str.lower().str.strip()
    departments = df.actual_position_title.str.extract(r'(jaile?r?|(dept)?\.?records|public information|'
                                                       r'communications?|patrolman|office|information tech)')
    df.loc[:, 'department_desc'] = departments[0]\
        .str.replace(r'dept\.records', 'records', regex=True)\
        .str.replace(r'jaile?r?', 'corrections', regex=True)\
        .str.replace('patrolman', 'patrol', regex=False)\
        .str.replace(r'communication$', 'communications', regex=True)\
        .str.replace('office', 'administration', regex=False)\
        .str.replace('information tech', 'it')
    return df 


def clean_rank_desc(df):
    df.loc[:, 'rank_desc'] = df.actual_position_title.str.lower().str.strip()\
        .str.replace(r'\.', '', regex=True).str.replace(r' ?-? ?(of)? ?police ?', '', regex=True)\
        .str.replace(r' ?d?e?p?t?records ?| \biv?i?i?\b|legal |'
                     r'\boffice\b |public information |information tech |'
                     r' ?(of)? ?communications? ?| \(part time\)|jail ', '', regex=True)\
        .str.replace(r'secretary.+', 'secretary', regex=True)\
        .str.replace(r' ?- ?', '', regex=True)\
        .str.replace(r'adm asst.+', 'administrative assistant', regex=True)\
        .str.replace(r'\bsuperviso\b', 'supervisor', regex=True)
    return df.drop(columns='actual_position_title')


def split_name(df):
    df.loc[:, 'employee_name'] = df.employee_name.str.lower().str.strip()
    names = df.employee_name.str.extract(r'(\w+-\w+) ?(iii|jr)?, (\w+) (.+)')
    df.loc[:, 'last_name'] = names[0]
    df.loc[:, 'suffix'] = names[1]
    df.loc[:, 'first_name'] = names[2]
    df.loc[:, 'middle_initial'] = names[3]
    return df 


def clean():
    df = pd.read_csv(data_file_path('raw/bossier_city_pd/bossiercity_pd_pprr_2019.csv'))\
        .pipe(clean_column_names)\
        .rename(columns={
            'employee_number': 'employee_id'
        })\
        .pipe(extract_department_desc)\
        .pipe(clean_rank_desc)\
        .pipe(split_name)\
        .pipe(clean_dates, ['birth_date', 'hire_date'])
    return df
