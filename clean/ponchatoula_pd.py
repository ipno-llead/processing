from lib.standardize import standardize_from_lookup_table
import sys

import pandas as pd

from lib.columns import clean_column_names, set_values
from lib.clean import clean_dates, clean_names, clean_salaries, clean_sexes, standardize_desc_cols
from lib.path import data_file_path, ensure_data_dir
from lib.uid import gen_uid

sys.path.append('../')


def clean_department_desc(df):
    df.loc[:, 'department_desc'] = df.department_desc.str.lower().str.strip()\
        .replace({
            'pol': 'police',
            'disp': 'communications'
        })
    return df


def clean_pprr():
    return pd.read_csv(data_file_path(
        'raw/ponchatoula_pd/ponchatoula_pd_pprr_2010_2020.csv'
    )).pipe(clean_column_names)\
        .drop(columns=['class', 'status_code'])\
        .rename(columns={
            'employee': 'employee_id',
            'middle_init': 'middle_initial',
            'regular_rate': 'salary',
            'work_center': 'department_desc',
        })\
        .pipe(clean_names, ['first_name', 'middle_initial', 'last_name'])\
        .pipe(clean_salaries, ['salary'])\
        .pipe(standardize_desc_cols, ['salary_freq'])\
        .pipe(clean_department_desc)\
        .pipe(clean_sexes, ['sex'])\
        .pipe(clean_dates, ['hire_date'])\
        .pipe(set_values, {
            'data_production_year': '2020',
            'agency': 'Ponchatoula PD'
        })\
        .pipe(gen_uid, ['agency', 'employee_id'])


def clean_allegation(df):
    df.loc[:, 'allegation'] = df.allegation.str.lower().str.strip()\
        .str.replace(r'\bcommens\b', 'comments', regex=True)\
        .str.replace(r'\bunbeomcing\b', 'unbecoming', regex=True)
    return standardize_from_lookup_table(df, 'allegation', [
        ['taking photos at crime scene'],
        ['missed city court'],
        ['no show for duty', 'no show'],
        ['manpower shortage on his shift'],
        ['late reports'],
        ['lack of productivity'],
        ['using cell phone in traffic'],
        ["lock out assist and scratched citizen's point of view"],
        ['allowing unauthorized personnel into restricted area'],
        ['alterted police report'],
        ['failed to transmit via police department radio of request for back up'],
        ['off duty in marked unit 19 parked in handicap area'],
        ['advised shift supervisor that he needed to be off to attend to personal business then worked a paid detail during same shift hours'],  # noqa
        ['unsafe driving of police department unit'],
        ['discourtesy', 'discourteous to co-workers'],
        ['improper comments to city hall employees'],
        ['offensive language'],
        ['insubordination'],
        ['conduct unbecoming'],
        # 'loss of police department property' 'lack of attention to radio calls'
        # 'punctuality/availability for duty' 'handling domestic dispute'
        # 'vehicle damage (light)' 'inattentive with detainee'
        # 'inappropriate use of emergency lights' 'personal domestic incident'
        # 'time sheet discrepancy' 'unnecessary restraint' 'unprofessionalism '
        # 'chemeical spraying of bystanders' 'misconduct with witness'
        # 'rights violation' 'rudeness/unprofessionalism' 'missed court'
        # 'unnecessary use of siren and emerging lights'
        # 'mistreated | excessive force' 'delayed report writing' 'lax supervision'
        # 'careless with police department equipment'
        # 'lax documentation on warrants' 'late returning from break'
        # 'failure to clear fta on stopped motorist'
        # 'improper maintenance of equipment'
        # "improper securing of arrestee's personal effects"
        # 'improper securing of evidence' 'failure to complete crash report'
        # 'minor damage to equipment' 'discourtest'
    ])


def clean_cprr():
    return pd.read_csv(data_file_path(
        'raw/ponchatoula_pd/ponchatoula_cprr_2010_2020.csv'
    )).pipe(clean_column_names)\
        .rename(columns={
            'charges': 'allegation'
        }).pipe(clean_allegation)


if __name__ == '__main__':
    df = clean_pprr()
    ensure_data_dir('clean')
    df.to_csv(data_file_path(
        'clean/pprr_ponchatoula_pd_2010_2020.csv'
    ), index=False)
