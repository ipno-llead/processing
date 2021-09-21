import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path
from lib.columns import clean_column_names, set_values
from lib.clean import standardize_desc_cols, clean_dates
from lib.uid import gen_uid


def clean_department(df):
    sub_departments = df.department.str.lower().str.strip().str.extract(r'-(\w+) (\d+)$')
    df.loc[:, 'sub_department'] = sub_departments[0] + ' ' + sub_departments[1]

    df.loc[:, 'department'] = df.department.str.lower().str.strip()\
        .str.replace(r'-district (\d+)', '', regex=True)\
        .str.replace('.', '', regex=False)
    return df.fillna('')


def clean_rank_desc(df):
    df.loc[:, 'rank_desc'] = df.position.str.lower().str.strip()\
        .str.replace(r'civil|purchasing|^civil ?| cri?m?i?n?a?l? records| ?(of)? ?correcti?ona?l?s? ?|'
                     r' ?prison ?| ?marii?ne ?| ?division ?| ?transportation ?|hr |'
                     r'traffii?c ?| ?domestic abuse|young|seniors|program|'
                     r'dare ?| ?reserve ?(divi?s?i?o?n?)?| range| task force|'
                     r'| ?patrolm?a?n? ?| ?training|maintenance| dispatch$|'
                     r'part ?time|full time|special services|^leader$|\/|-|\.', '', regex=True)\
        .str.replace(r'(\w+) +(\w+)', r'\1 \2', regex=True)\
        .str.replace(r'\bdy\b', 'deputy', regex=True)\
        .str.replace('deputy chief', 'chief deputy', regex=False)\
        .str.replace(r'\basst\b', 'assistant', regex=True)\
        .str.replace(r'supe?rvi?sor', 'supervisor', regex=True)\
        .str.replace('sgt', 'sergeant', regex=False)\
        .str.replace(' civilpurchasing', '', regex=False)\
        .str.replace('secretarycase', 'secretary case', regex=False)\
        .str.replace('parttime', '', regex=False)\
        .str.replace('code enforcement', 'code enforcer', regex=False)\
        .str.replace('communications', '', regex=False)\
        .str.replace(r'operator$', 'dispatcher', regex=True)
    return df.drop(columns='position')



def split_names(df):
    col_name = [col for col in df.columns if col.endswith('employee_name')][0]
    names = df[col_name].str.strip().str.lower()\
        .str.extract(r'^(\w+)[ ,]+(\w+)$')
    df.loc[:, 'first_name'] = names[1]
    df.loc[:, 'last_name'] = names[0]
    return df.drop(columns=[col_name])

def split_name(df):
    df.loc[:, 'name'] = df.name.str.lower().str.strip()\
        .str.replace(',', ' ', regex=False)\
        .str.replace(r' +', ' ', regex=True)\
        .str.replace(r'(\w+) (\w+) ?(?:(\w{1}) )? ?(?:(\w+))?', r'\1 \2 \4 \3', regex=True)
    names = df.name.str.extract(r'(?:(\w+)) (?:(\w+) ) ?(?:(\w{2,3}) )? ?(?:(\w+) )?')
    df.loc[:, 'first_name'] = names[1]
    df.loc[:, 'last_names'] = names[0] 
    df.loc[:, 'suffixes'] = names[2].fillna('')
    df.loc[:, 'middle_name'] = names.loc[:, 3].str.strip().fillna('')\
        .map(lambda s: '' if len(s) < 5 else s)
    df.loc[:, 'middle_initial'] = names.loc[:, 3].str.strip().fillna('')\
        .map(lambda s: '' if len(s) > 1 else s)
    df.loc[:, 'last_name'] = df.last_names.fillna('') + ' ' + df.suffixes.fillna('')
    return df.drop(columns={'last_names', 'suffixes'})


## there are duplicate badge_nos


def assign_agency(df):
    df.loc[:, 'agency'] = 'Plaquemines SO'


def clean():
    df = pd.read_csv(data_file_path('raw/plaquemines_so/plaquemines_so_pprr_2018.csv'))
    df = df\
        .pipe(clean_column_names)\
        .rename(columns={
            'birth_date': 'birth_year',
            'emp_type': 'employment_status'
        })\
        .pipe(clean_department)\
        .pipe(clean_rank_desc)\
        .pipe(split_name)\
        .pipe(clean_dates, ['hire_date'])\
        .pipe(standardize_desc_cols, ['employment_status', 'rank_desc'])\
        .pipe(set_values, {
            'agency': 'Plaqumines SO'})\
        .pipe(gen_uid, ['first_name', 'last_name', 'middle_initial', 'middle_name', 'agency'])
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(data_file_path(
        'clean/pprr_plaquemines_so_2018.csv'), index=False)
